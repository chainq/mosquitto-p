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

function mqttinit: boolean;
function loglevel_to_str(const loglevel: cint): ansistring;

type
  TMQTTOnMessageEvent = procedure(const payload: Pmosquitto_message) of object;
  TMQTTOnPublishEvent = procedure(const mid: cint) of object;
  TMQTTOnSubscribeEvent = procedure(mid: cint; qos_count: cint; const granted_qos: pcint) of object;
  TMQTTOnUnsubscribeEvent = procedure(mid: cint) of object;
  TMQTTOnConnectEvent = procedure(const rc: cint) of object;
  TMQTTOnDisconnectEvent = procedure(const rc: cint) of object;
  TMQTTOnLogEvent = procedure(const level: cint; const str: ansistring) of object;

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
      FAutoReconnect: boolean;
      FMQTTState: TMQTTConnectionState;
      FMQTTStateLock: TRTLCriticalSection;
      function GetMQTTState: TMQTTConnectionState;
      procedure SetMQTTState(state: TMQTTConnectionState);
      procedure Execute; override;
    public
      constructor Create(const name: string; const config: TMQTTConfig);
      destructor Destroy; override;
      function Connect: cint;
      function Reconnect: cint;
      function Subscribe(var mid: cint; const sub: ansistring; qos: cint): cint;
      function Subscribe(const sub: ansistring; qos: cint): cint;
      function Publish(var mid: cint; const topic: ansistring; payloadlen: cint; var payload; qos: cint; retain: cbool): cint;
      function Publish(const topic: ansistring; payloadlen: cint; var payload; qos: cint; retain: cbool): cint;
      function Publish(var mid: cint; const topic: ansistring; const payload: ansistring; qos: cint; retain: cbool): cint;
      function Publish(const topic: ansistring; const payload: ansistring; qos: cint; retain: cbool): cint;

      property State: TMQTTConnectionState read GetMQTTState write SetMQTTState;
      property AutoReconnect: boolean read FAutoReconnect write FAutoReconnect;
      property OnMessage: TMQTTOnMessageEvent read FOnMessage write FOnMessage;
      property OnPublish: TMQTTOnPublishEvent read FOnPublish write FOnPublish;
      property OnSubscribe: TMQTTOnSubscribeEvent read FOnSubscribe write FOnSubscribe;
      property OnUnsubscribe: TMQTTOnUnsubscribeEvent read FOnUnsubscribe write FOnUnsubscribe;
      property OnConnect: TMQTTOnConnectEvent read FOnConnect write FOnConnect;
      property OnDisconnect: TMQTTOnDisconnectEvent read FOnDisconnect write FOnDisconnect;
      property OnLog: TMQTTOnLogEvent read FOnLog write FOnLog;
  end;

implementation


uses
  sysutils;

procedure mqtt_on_message(mosq: Pmosquitto; obj: pointer; const message: Pmosquitto_message); cdecl; forward;
procedure mqtt_on_publish(mosq: Pmosquitto; obj: pointer; mid: cint); cdecl; forward;
procedure mqtt_on_subscribe(mosq: Pmosquitto; obj: pointer; mid: cint; qos_count: cint; const granted_qos: pcint); cdecl; forward;
procedure mqtt_on_unsubscribe(mosq: Pmosquitto; obj: pointer; mid: cint); cdecl; forward;
procedure mqtt_on_connect(mosq: Pmosquitto; obj: pointer; rc: cint); cdecl; forward;
procedure mqtt_on_disconnect(mosq: Pmosquitto; obj: pointer; rc: cint); cdecl; forward;
procedure mqtt_on_log(mosq: Pmosquitto; obj: pointer; level: cint; const str: pchar); cdecl; forward;


constructor TMQTTConnection.Create(const name: String; const config: TMQTTConfig);
var
  rc: cint;
begin
  inherited Create(true);

  FName:=name;
  FMosquitto:=mosquitto_new(nil, true, self);
  if FMosquitto=nil then
    raise Exception.Create('Mosquitto Error');

  FConfig:=config;
  FAutoReconnect:=true;
  InitCriticalSection(FMQTTStateLock);
  State:=mqttNone;

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
      rc:=mosquitto_tls_set(Fmosquitto, PChar(ExtractFileName(FConfig.ssl_cacertfile)), PChar(ExtractFilePath(FConfig.ssl_cacertfile)), nil, nil, nil);
      if rc <> MOSQ_ERR_SUCCESS then
        begin
          writeln('[MQTT] TLS setup error: ',mosquitto_strerror(rc));
          raise Exception.Create('TLS Error');
        end;
    end;

  if FConfig.username <> '' then
    mosquitto_username_pw_set(Fmosquitto, PChar(FConfig.username), PChar(FConfig.password));

  Start; { ... the thread }
end;

destructor TMQTTConnection.Destroy;
begin
  mosquitto_disconnect(FMosquitto);

  Terminate; { ... the thread }
  WaitFor;

  mosquitto_destroy(FMosquitto);
  FMosquitto:=nil;

  DoneCriticalSection(FMQTTStateLock);

  inherited;
end;

function TMQTTConnection.Connect: cint;
begin
  writeln('[MQTT] [',FName,'] Connecting to [',FConfig.hostname,':',FConfig.port,'] - SSL:',FConfig.SSL);
  result:=mosquitto_connect_async(Fmosquitto, PChar(FConfig.hostname), FConfig.port, FConfig.keepalives);
  if result = MOSQ_ERR_SUCCESS then
    State:=mqttConnecting;
end;

function TMQTTConnection.Reconnect: cint;
begin
  result:=mosquitto_reconnect_async(Fmosquitto);
  if result = MOSQ_ERR_SUCCESS then
    State:=mqttConnecting;
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

function TMQTTConnection.GetMQTTState: TMQTTConnectionState;
begin
  EnterCriticalSection(FMQTTStateLock);
  result:=FMQTTState;
  LeaveCriticalSection(FMQTTStateLock);
end;

procedure TMQTTConnection.SetMQTTState(state: TMQTTConnectionState);
begin
  EnterCriticalSection(FMQTTStateLock);
  writeln('[MQTT] [',FName,'] State change: ',FMQTTState,' -> ',state);
  FMQTTState:=state;
  LeaveCriticalSection(FMQTTStateLock);
end;

procedure TMQTTConnection.Execute;
begin
  try
    writeln('[MQTTTHREAD] Entering thread...');
    { OK, so this piece has to manage the entire reconnecting logic, because
      libmosquitto only has the reconnection logic and state machine, if the
      code uses the totally blocking mosquitto_loop_forever(), which has a
      few quirks to say the least, plus due to its blocking nature, has
      a few interoperability issues with Pascal threading... (CB) }
    while not (Terminated and (State = mqttDisconnected)) do
      begin
        case State of
          mqttNone:
            Sleep(100);
          mqttDisconnected:
            if AutoReconnect then
              State:=mqttReconnecting
            else
              State:=mqttNone;
          mqttReconnecting:
            begin
              Sleep(100); //FIXME: proper backoff reconnect delay here!
              Reconnect;
            end;
          mqttConnected, mqttConnecting:
            mosquitto_loop(Fmosquitto, 100, 1);
        end;
      end;
  except
    writeln('[MQTTTHREAD] Exception!');
  end;
  writeln('[MQTTTHREAD] Exiting thread.');
end;




function loglevel_to_str(const loglevel: cint): ansistring;
begin
  loglevel_to_str:='UNKNOWN';
  case loglevel of
    MOSQ_LOG_INFO: loglevel_to_str:='INFO';
    MOSQ_LOG_NOTICE: loglevel_to_str:='NOTICE';
    MOSQ_LOG_WARNING: loglevel_to_str:='WARNING';
    MOSQ_LOG_ERR: loglevel_to_str:='ERROR';
    MOSQ_LOG_DEBUG: loglevel_to_str:='DEBUG';
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
        writeln('[MQTT] [',FName,'] Publish ID: ',mid);
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
        writeln('[MQTT] [',FName,'] Broker connection: ',mosquitto_strerror(rc));
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
        writeln('[MQTT] [',FName,'] Broker disconnected: ',disconnect_reason[rc = 0]);
        State:=mqttDisconnected;
        if assigned(OnDisconnect) then
          OnDisconnect(rc);
      end;
end;

procedure mqtt_on_log(mosq: Pmosquitto; obj: pointer; level: cint; const str: pchar); cdecl;
var
  Fmosquitto: TMQTTConnection absolute obj;
  name: String;
begin
  name:='';
  if assigned(Fmosquitto) then
    name:=FMosquitto.FName;
  if name='' then
    name:='UNNAMED';

  writeln('[MOSQUITTO] [',name,'] ',loglevel_to_str(level),' ',str);

  if assigned(Fmosquitto) and assigned(FMosquitto.OnLog) then
    Fmosquitto.OnLog(level, str);
end;

var
  libinited: boolean;


function mqttinit: boolean;
var
  major, minor, revision: cint;
begin
  result:=libinited;
  if not libinited then
    begin
      writeln('[MOSQ] mosquitto init failed.');
      exit;
    end;

  mosquitto_lib_version(@major,@minor,@revision);

  writeln('[MQTT] Compiled against mosquitto header version ',LIBMOSQUITTO_MAJOR,'.',LIBMOSQUITTO_MINOR,'.',LIBMOSQUITTO_REVISION);
  writeln('[MQTT] Running against libmosquitto version ',major,'.',minor,'.',revision);
end;

initialization
  libinited:=mosquitto_lib_init = MOSQ_ERR_SUCCESS;
finalization
  if libinited then
    mosquitto_lib_cleanup;
end.
