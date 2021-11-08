{

  Copyright (c) 2018-2019  Karoly Balogh <charlie@amigaspirit.hu>

  Permission to use, copy, modify, and/or distribute this software for
  any purpose with or without fee is hereby granted, provided that the
  above copyright notice and this permission notice appear in all copies.

  THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL
  WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED
  WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
  THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR
  CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
  LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT,
  NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
  CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

}

{$MODE OBJFPC}
unit mqttclass;

interface

uses
  classes, ctypes, mosquitto;

type
  mqtt_logfunc = procedure(const msg: ansistring);

function mqtt_init: boolean; overload;
function mqtt_init(const verbose: boolean): boolean; overload;
function mqtt_loglevel_to_str(const loglevel: cint): ansistring;
procedure mqtt_setlogfunc(logfunc: mqtt_logfunc);

type
  TMQTTOnMessageEvent = procedure(const payload: Pmosquitto_message) of object;
  TMQTTOnPublishEvent = procedure(const mid: cint) of object;
  TMQTTOnSubscribeEvent = procedure(mid: cint; qos_count: cint; const granted_qos: pcint) of object;
  TMQTTOnUnsubscribeEvent = procedure(mid: cint) of object;
  TMQTTOnConnectEvent = procedure(const rc: cint) of object;
  TMQTTOnDisconnectEvent = procedure(const rc: cint) of object;
  TMQTTOnLogEvent = procedure(const level: cint; const str: ansistring) of object;

const
  MOSQ_LOG_NODEBUG = MOSQ_LOG_ALL - MOSQ_LOG_DEBUG;

type
  TMQTTConnectionState = ( mqttNone, mqttConnecting, mqttConnected, mqttReconnecting, mqttDisconnected );

type
  TMQTTConfig = record
    ssl: boolean;
    ssl_cacertfile: ansistring;
    hostname: ansistring;
    port: word;
    username: ansistring;
    password: ansistring;
    keepalives: longint;
    reconnect_delay: longint;
    reconnect_backoff: boolean;
    client_id: ansistring;
  end;

type
  { This junk uses FPC-supplied threading, because mosquitto for whatever retarded reason
    on Windows comes w/o threading enabled by default (CB) }
  TMQTTConnection = class(TThread)
    protected
      FName: string;
      FMosquitto: Pmosquitto;
      FConfig: TMQTTConfig;

      FOnMessage: TMQTTOnMessageEvent;
      FOnPublish: TMQTTOnPublishEvent;
      FOnSubscribe: TMQTTOnSubscribeEvent;
      FOnUnsubscribe: TMQTTOnUnsubscribeEvent;
      FOnConnect: TMQTTOnConnectEvent;
      FOnDisconnect: TMQTTOnDisconnectEvent;
      FOnLog: TMQTTOnLogEvent;
      FMQTTLogLevel: cint; { allowed log levels of the mqttclass }
      FMOSQLogLevel: cint; { allowed log levels of the underlying mosquitto lib }

      FAutoReconnect: boolean;
      FReconnectTimer: TDateTime;
      FReconnectPeriod: longint;
      FReconnectDelay: longint;
      FReconnectBackoff: boolean;

      FMQTTState: TMQTTConnectionState;
      FMQTTStateLock: TRTLCriticalSection;

      procedure SetupReconnectDelay;
      function ReconnectDelayExpired: boolean;

      function GetMQTTState: TMQTTConnectionState;
      procedure SetMQTTState(state: TMQTTConnectionState);
      procedure Execute; override;
    public
      constructor Create(const name: string; const config: TMQTTConfig);
      constructor Create(const name: string; const config: TMQTTConfig; const loglevel: cint);
      destructor Destroy; override;
      function Connect: cint;
      function Reconnect: cint;
      function Subscribe(var mid: cint; const sub: ansistring; qos: cint): cint;
      function Subscribe(const sub: ansistring; qos: cint): cint;
      function Publish(var mid: cint; const topic: ansistring; payloadlen: cint; var payload; qos: cint; retain: cbool): cint;
      function Publish(const topic: ansistring; payloadlen: cint; var payload; qos: cint; retain: cbool): cint;
      function Publish(var mid: cint; const topic: ansistring; const payload: ansistring; qos: cint; retain: cbool): cint;
      function Publish(const topic: ansistring; const payload: ansistring; qos: cint; retain: cbool): cint;

      procedure Log(const level: cint; const message: ansistring);

      property State: TMQTTConnectionState read GetMQTTState write SetMQTTState;
      property AutoReconnect: boolean read FAutoReconnect write FAutoReconnect;
      property OnMessage: TMQTTOnMessageEvent read FOnMessage write FOnMessage;
      property OnPublish: TMQTTOnPublishEvent read FOnPublish write FOnPublish;
      property OnSubscribe: TMQTTOnSubscribeEvent read FOnSubscribe write FOnSubscribe;
      property OnUnsubscribe: TMQTTOnUnsubscribeEvent read FOnUnsubscribe write FOnUnsubscribe;
      property OnConnect: TMQTTOnConnectEvent read FOnConnect write FOnConnect;
      property OnDisconnect: TMQTTOnDisconnectEvent read FOnDisconnect write FOnDisconnect;
      property OnLog: TMQTTOnLogEvent read FOnLog write FOnLog;

      property MQTTLogLevel: cint read FMQTTLogLevel write FMQTTLogLevel;
      property MOSQLogLevel: cint read FMOSQLogLevel write FMOSQLogLevel;
  end;

implementation


uses
  sysutils, dateutils;

const
  DEFAULT_RECONNECT_PERIOD_MS = 100;
  DEFAULT_RECONNECT_DELAY_MS = 60 * 1000;

var
  logger: mqtt_logfunc;

procedure mqtt_on_message(mosq: Pmosquitto; obj: pointer; const message: Pmosquitto_message); cdecl; forward;
procedure mqtt_on_publish(mosq: Pmosquitto; obj: pointer; mid: cint); cdecl; forward;
procedure mqtt_on_subscribe(mosq: Pmosquitto; obj: pointer; mid: cint; qos_count: cint; const granted_qos: pcint); cdecl; forward;
procedure mqtt_on_unsubscribe(mosq: Pmosquitto; obj: pointer; mid: cint); cdecl; forward;
procedure mqtt_on_connect(mosq: Pmosquitto; obj: pointer; rc: cint); cdecl; forward;
procedure mqtt_on_disconnect(mosq: Pmosquitto; obj: pointer; rc: cint); cdecl; forward;
procedure mqtt_on_log(mosq: Pmosquitto; obj: pointer; level: cint; const str: pchar); cdecl; forward;

procedure mqtt_setlogfunc(logfunc: mqtt_logfunc);
begin
  logger:=logfunc;
end;

procedure mqtt_log(const msg: ansistring);
begin
  writeln(msg);
end;

constructor TMQTTConnection.Create(const name: String; const config: TMQTTConfig);
begin
  Create(name, config, MOSQ_LOG_ALL);
end;

constructor TMQTTConnection.Create(const name: String; const config: TMQTTConfig; const loglevel: cint);
var
  rc: cint;
begin
  inherited Create(true);

  FName:=name;
  FConfig:=config;

  FMosquitto:=mosquitto_new(PChar(@FConfig.client_id[1]), true, self);
  if FMosquitto=nil then
    raise Exception.Create('mosquitto instance creation failure');

  FAutoReconnect:=true;
  InitCriticalSection(FMQTTStateLock);
  State:=mqttNone;

  MQTTLogLevel:=loglevel;
  MOSQLogLevel:=loglevel;

  FReconnectTimer:=Now;
  FReconnectPeriod:=DEFAULT_RECONNECT_PERIOD_MS;
  FReconnectDelay:=DEFAULT_RECONNECT_DELAY_MS;
  if FConfig.reconnect_delay <> 0 then
    FReconnectDelay:=FConfig.reconnect_delay * 1000
  else
    FAutoReconnect:=false;
  FReconnectBackoff:=FConfig.reconnect_backoff;

  mosquitto_threaded_set(Fmosquitto, true);

  mosquitto_log_callback_set(FMosquitto, @mqtt_on_log);
  mosquitto_message_callback_set(FMosquitto, @mqtt_on_message);
  mosquitto_publish_callback_set(FMosquitto, @mqtt_on_publish);
  mosquitto_subscribe_callback_set(FMosquitto, @mqtt_on_subscribe);
  mosquitto_unsubscribe_callback_set(FMosquitto, @mqtt_on_unsubscribe);
  mosquitto_connect_callback_set(FMosquitto, @mqtt_on_connect);
  mosquitto_disconnect_callback_set(FMosquitto, @mqtt_on_disconnect);

  if FConfig.ssl then
    begin
      if (FConfig.ssl_cacertfile <> '') then
        begin
          Log(MOSQ_LOG_INFO,'TLS CERT: '+FConfig.ssl_cacertfile);
          rc:=mosquitto_tls_set(Fmosquitto, PChar(FConfig.ssl_cacertfile), nil, nil, nil, nil);
          if rc <> MOSQ_ERR_SUCCESS then
            begin
              Log(MOSQ_LOG_ERR,'TLS Setup: '+mosquitto_strerror(rc));
              raise Exception.Create('TLS Setup: '+mosquitto_strerror(rc));
            end;
        end
      else
        Log(MOSQ_LOG_WARNING,'SSL enabled, but no CERT file specified. Skipping TLS setup...');
    end;

  if FConfig.username <> '' then
    mosquitto_username_pw_set(Fmosquitto, PChar(FConfig.username), PChar(FConfig.password));

  Start; { ... the thread }
end;

destructor TMQTTConnection.Destroy;
begin
  mosquitto_disconnect(FMosquitto);

  if not Suspended then
    begin
      Terminate; { ... the thread }
      WaitFor;
    end;

  mosquitto_destroy(FMosquitto);
  FMosquitto:=nil;

  DoneCriticalSection(FMQTTStateLock);

  inherited;
end;

function TMQTTConnection.Connect: cint;
begin
  Log(MOSQ_LOG_INFO,'Connecting to ['+FConfig.hostname+':'+IntToStr(FConfig.port)+'] - SSL:'+BoolToStr(FConfig.SSL,true));
  result:=mosquitto_connect_async(Fmosquitto, PChar(FConfig.hostname), FConfig.port, FConfig.keepalives);
  if result = MOSQ_ERR_SUCCESS then
    State:=mqttConnecting
  else
    begin
      State:=mqttDisconnected;
      Log(MOSQ_LOG_ERR,'Connection failed with: '+mosquitto_strerror(result));
    end;
end;

function TMQTTConnection.Reconnect: cint;
begin
  Log(MOSQ_LOG_INFO,'Reconnecting to ['+FConfig.hostname+':'+IntToStr(FConfig.port)+'] - SSL:'+BoolToStr(FConfig.SSL,true));
  result:=mosquitto_reconnect_async(Fmosquitto);
  if result = MOSQ_ERR_SUCCESS then
    State:=mqttConnecting
  else
    begin
      State:=mqttDisconnected;
      Log(MOSQ_LOG_ERR,'Reconnection failed with: '+mosquitto_strerror(result));
    end;
end;

function TMQTTConnection.Subscribe(var mid: cint; const sub: ansistring; qos: cint): cint;
begin
  result:=mosquitto_subscribe(Fmosquitto, @mid, PChar(sub), qos);
end;

function TMQTTConnection.Subscribe(const sub: ansistring; qos: cint): cint;
begin
  result:=mosquitto_subscribe(Fmosquitto, nil, PChar(sub), qos);
end;


function TMQTTConnection.Publish(var mid: cint; const topic: ansistring; payloadlen: cint; var payload; qos: cint; retain: cbool): cint;
begin
  result:=mosquitto_publish(Fmosquitto, @mid, PChar(topic), payloadlen, @payload, qos, retain);
end;

function TMQTTConnection.Publish(const topic: ansistring; payloadlen: cint; var payload; qos: cint; retain: cbool): cint;
begin
  result:=mosquitto_publish(Fmosquitto, nil, PChar(topic), payloadlen, @payload, qos, retain);
end;

function TMQTTConnection.Publish(var mid: cint; const topic: ansistring; const payload: ansistring; qos: cint; retain: cbool): cint;
begin
  result:=mosquitto_publish(Fmosquitto, @mid, PChar(topic), length(payload), PChar(payload), qos, retain);
end;

function TMQTTConnection.Publish(const topic: ansistring; const payload: ansistring; qos: cint; retain: cbool): cint;
begin
  result:=mosquitto_publish(Fmosquitto, nil, PChar(topic), length(payload), PChar(payload), qos, retain);
end;


procedure TMQTTConnection.Log(const level: cint; const message: ansistring);
var
  tmp: ansistring;
begin
  if (MQTTLogLevel and level) > 0 then
    begin
      writestr(tmp,'[MQTT] [',FName,'] ',mqtt_loglevel_to_str(level),' ',message);
      logger(tmp);
    end;
end;


function TMQTTConnection.GetMQTTState: TMQTTConnectionState;
begin
  EnterCriticalSection(FMQTTStateLock);
  result:=FMQTTState;
  LeaveCriticalSection(FMQTTStateLock);
end;

procedure TMQTTConnection.SetMQTTState(state: TMQTTConnectionState);
var
  tmp: ansistring;
begin
  EnterCriticalSection(FMQTTStateLock);
  if FMQTTState <> state then
    begin
      writestr(tmp,'State change: ',FMQTTState,' -> ',state);
      Log(MOSQ_LOG_DEBUG,tmp);
      FMQTTState:=state;
    end;
  LeaveCriticalSection(FMQTTStateLock);
end;

procedure TMQTTConnection.SetupReconnectDelay;
begin
  if FReconnectBackoff then
    begin
      { This is kind of a kludge, but I've got no better idea for a simple
        solution - if there was no reconnection attempt for the double of
        the reconnect delay, we reset the backoff on the next attempt. (CB) }
      if MillisecondsBetween(FReconnectTimer, Now) > (FReconnectDelay * 2) then
        FReconnectPeriod:=DEFAULT_RECONNECT_PERIOD_MS
      else
        FReconnectPeriod:=FReconnectPeriod * 2;
    end
  else
    FReconnectPeriod:=FReconnectDelay;

  if FReconnectPeriod > FReconnectDelay then
    FReconnectPeriod:=FReconnectDelay;

  FReconnectTimer:=Now;
  Log(MOSQ_LOG_INFO,'Next reconnection attempt in '+FloatToStr(FReconnectPeriod / 1000)+' seconds.');
end;

function TMQTTConnection.ReconnectDelayExpired: boolean;
begin
  result:=MillisecondsBetween(FReconnectTimer, Now) > FReconnectPeriod;
end;


procedure TMQTTConnection.Execute;
begin
  try
    Log(MOSQ_LOG_DEBUG,'Entering subthread...');
    { OK, so this piece has to manage the entire reconnecting logic, because
      libmosquitto only has the reconnection logic and state machine, if the
      code uses the totally blocking mosquitto_loop_forever(), which has a
      few quirks to say the least, plus due to its blocking nature, has
      a few interoperability issues with Pascal threading... (CB) }
    while not (Terminated and (State in [mqttDisconnected, mqttReconnecting, mqttNone ] )) do
      begin
        case State of
          mqttNone:
            Sleep(100);
          mqttDisconnected:
            if AutoReconnect then
              begin
                SetupReconnectDelay;
                State:=mqttReconnecting;
              end
            else
              begin
                Log(MOSQ_LOG_INFO,'Automatic reconnection disabled, going standby.');
                State:=mqttNone;
              end;
          mqttReconnecting:
            begin
              if ReconnectDelayExpired then
                Reconnect
              else
                Sleep(100);
            end;
          mqttConnected, mqttConnecting:
            mosquitto_loop(Fmosquitto, 100, 1);
        end;
      end;
    State:=mqttNone;
  except
    { FIX ME: this really needs something better }
    Log(MOSQ_LOG_ERR,'Exception in subthread, leaving...');
  end;
  Log(MOSQ_LOG_DEBUG,'Exiting subthread.');
end;




function mqtt_loglevel_to_str(const loglevel: cint): ansistring;
begin
  mqtt_loglevel_to_str:='UNKNOWN';
  case loglevel of
    MOSQ_LOG_INFO: mqtt_loglevel_to_str:='INFO';
    MOSQ_LOG_NOTICE: mqtt_loglevel_to_str:='NOTICE';
    MOSQ_LOG_WARNING: mqtt_loglevel_to_str:='WARNING';
    MOSQ_LOG_ERR: mqtt_loglevel_to_str:='ERROR';
    MOSQ_LOG_DEBUG: mqtt_loglevel_to_str:='DEBUG';
  end;
end;


procedure mqtt_on_message(mosq: Pmosquitto; obj: pointer; const message: Pmosquitto_message); cdecl;
var
  Fmosquitto: TMQTTConnection absolute obj;
begin
  if assigned(Fmosquitto) and assigned(FMosquitto.OnMessage) then
    Fmosquitto.OnMessage(message);
end;

procedure mqtt_on_publish(mosq: Pmosquitto; obj: pointer; mid: cint); cdecl;
var
  Fmosquitto: TMQTTConnection absolute obj;
begin
  if assigned(Fmosquitto) then
    with FMosquitto do
      begin
        Log(MOSQ_LOG_DEBUG,'Publish ID: '+IntToStr(mid));
        if assigned(OnPublish) then
          OnPublish(mid);
      end;
end;

procedure mqtt_on_subscribe(mosq: Pmosquitto; obj: pointer; mid: cint; qos_count: cint; const granted_qos: pcint); cdecl;
var
  FMosquitto: TMQTTConnection absolute obj;
begin
  if assigned(FMosquitto) and assigned(FMosquitto.OnSubscribe) then
    Fmosquitto.OnSubscribe(mid, qos_count, granted_qos);
end;

procedure mqtt_on_unsubscribe(mosq: Pmosquitto; obj: pointer; mid: cint); cdecl;
var
  Fmosquitto: TMQTTConnection absolute obj;
begin
  if assigned(FMosquitto) and assigned(FMosquitto.OnUnsubscribe) then
    FMosquitto.OnUnsubscribe(mid);
end;

procedure mqtt_on_connect(mosq: Pmosquitto; obj: pointer; rc: cint); cdecl;
var
  FMosquitto: TMQTTConnection absolute obj;
begin
  if assigned(FMosquitto) then
    with FMosquitto do
      begin
        Log(MOSQ_LOG_INFO,'Broker connection: '+mosquitto_strerror(rc));
        State:=mqttConnected;
        if assigned(OnConnect) then
          OnConnect(rc);
      end;
end;

procedure mqtt_on_disconnect(mosq: Pmosquitto; obj: pointer; rc: cint); cdecl;
const
  disconnect_reason: array[boolean] of string[31] = ('Connection lost', 'Normal termination');
var
  Fmosquitto: TMQTTConnection absolute obj;
begin
  if assigned(FMosquitto) then
    with FMosquitto do
      begin
        Log(MOSQ_LOG_INFO,'Broker disconnected: '+disconnect_reason[rc = 0]);
        State:=mqttDisconnected;
        if assigned(OnDisconnect) then
          OnDisconnect(rc);
      end;
end;

procedure mqtt_on_log(mosq: Pmosquitto; obj: pointer; level: cint; const str: pchar); cdecl;
var
  Fmosquitto: TMQTTConnection absolute obj;
  tmp: ansistring;
begin
  tmp:='';

  if assigned(Fmosquitto) then
    with Fmosquitto do
      begin
        if (MOSQLogLevel and level) > 0 then
          begin
            writestr(tmp,'[MOSQUITTO] [',FName,'] ',mqtt_loglevel_to_str(level),' ',str);
            logger(tmp)
          end;
        if assigned(OnLog) then
          OnLog(level, str);
      end
  else
    begin
      writestr(tmp,'[MOSQUITTO] [UNNAMED] ',mqtt_loglevel_to_str(level),' ',str);
      logger(tmp);
    end;
end;

var
  libinited: boolean;


function mqtt_init: boolean;
begin
  result:=mqtt_init(true);
end;

function mqtt_init(const verbose: boolean): boolean;
var
  major, minor, revision: cint;
begin
  result:=libinited;
  if not libinited then
    begin
      if verbose then
        logger('[MOSQUITTO] mosquitto init failed.');
      exit;
    end;

  if verbose then
    begin
      mosquitto_lib_version(@major,@minor,@revision);

      logger('[MQTT] Compiled against mosquitto header version '+IntToStr(LIBMOSQUITTO_MAJOR)+'.'+IntToStr(LIBMOSQUITTO_MINOR)+'.'+IntToStr(LIBMOSQUITTO_REVISION));
      logger('[MQTT] Running against libmosquitto version '+IntToStr(major)+'.'+IntToStr(minor)+'.'+IntToStr(revision));
    end;
end;

initialization
  libinited:=mosquitto_lib_init = MOSQ_ERR_SUCCESS;
  logger:=@mqtt_log;
finalization
  if libinited then
    mosquitto_lib_cleanup;
end.
