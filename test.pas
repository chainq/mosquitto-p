{
  Subscribing to all topics and printing out incoming messages
  Simple test code for the libmosquitto C API interface unit

  Copyright (c) 2019  Karoly Balogh <charlie@amigaspirit.hu>

  See the LICENSE file for licensing details.
}

program test;

uses
  mosquitto, ctypes;

const
  MQTT_HOST = 'localhost';
  MQTT_PORT = 1883;

var
  major, minor, revision: cint;
  mq: Pmosquitto;


procedure mqtt_on_log(mosq: Pmosquitto; obj: pointer; level: cint; const str: pchar); cdecl;
begin
  writeln(str);
end;

procedure mqtt_on_message(mosq: Pmosquitto; obj: pointer; const message: Pmosquitto_message); cdecl;
var
  msg: ansistring;
begin
  msg:='';
  with message^ do
    begin
      { Note that MQTT messages can be binary, but for this test case
        we just assume they're printable text }
      SetLength(msg,payloadlen);
      Move(payload^,msg[1],payloadlen);
      writeln('Topic: [',topic,'] - Message: [',msg,']');
    end;
end;

{ Here we really just use libmosquitto's C API directly, so see its documentation. }
begin
  if mosquitto_lib_init <> MOSQ_ERR_SUCCESS then
    begin
      writeln('Failed.');
      halt(1);
    end;
  mosquitto_lib_version(@major,@minor,@revision);

  writeln('Running against libmosquitto ',major,'.',minor,'.',revision);

  mq:=mosquitto_new(nil, true, nil);
  if assigned(mq) then
    begin
      mosquitto_log_callback_set(mq, @mqtt_on_log);
      mosquitto_message_callback_set(mq, @mqtt_on_message);

      mosquitto_connect(mq, MQTT_HOST, MQTT_PORT, 60);
      mosquitto_subscribe(mq, nil, '#', 1);

      while mosquitto_loop(mq, 100, 1) = MOSQ_ERR_SUCCESS do
        begin
          { This should ideally handle some keypress or something... }
        end;

      mosquitto_disconnect(mq);
      mosquitto_destroy(mq);
      mq:=nil;
    end
  else
    writeln('ERROR: Cannot create a mosquitto instance.');

  mosquitto_lib_cleanup;
end.
