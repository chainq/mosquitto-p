{
  Subscribing to all topics and printing out incoming messages
  Simple test code for the mqttclass unit

  Copyright (c) 2019  Karoly Balogh <charlie@amigaspirit.hu>

  See the LICENSE file for licensing details.
}

{$MODE OBJFPC}
program testclass;

uses
{$IFDEF HASUNIX}
  cthreads,
{$ENDIF}
  ctypes, mosquitto, mqttclass;

type
  TMyMQTTConnection = class(TMQTTConnection)
    procedure MyOnMessage(const payload: Pmosquitto_message);
  end;

procedure TMyMQTTConnection.MyOnMessage(const payload: Pmosquitto_message);
var
  msg: ansistring;
begin
  msg:='';
  with payload^ do
    begin
      { Note that MQTT messages can be binary, but for this test case we just
        assume they're printable text, as a test }
      SetLength(msg,payloadlen);
      Move(payload^,msg[1],payloadlen);
      writeln('Topic: [',topic,'] - Message: [',msg,']');
    end;
end;

var
  mqtt: TMyMQTTConnection;
  config: TMQTTConfig;

begin
  writeln('Press ENTER to quit.');

  FillChar(config, sizeof(config), 0);
  with config do
    begin
      port:=1883;
      hostname:='localhost';
      keepalives:=60;
    end;

  mqtt:=TMyMQTTConnection.Create('TEST',config);
  try
    { This could also go to a custom constructor of the class,
      for more complicated setups. }
    mqtt.OnMessage:=@mqtt.MyOnMessage;

    mqtt.Connect;
    mqtt.Subscribe('#',0); { Subscribe to all topics }

    readln;
  except
  end;
  mqtt.Free;

end.
