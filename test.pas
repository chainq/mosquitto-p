program test;

uses
  mosquitto, ctypes;

var
   major, minor, revision: cint;

begin
  if mosquitto_lib_init <> MOSQ_ERR_SUCCESS then
    begin
      writeln('Failed.');
      halt(1);
    end;
  mosquitto_lib_version(@major,@minor,@revision);

  writeln('Running against libmosquitto ',major,'.',minor,'.',revision);

  mosquitto_lib_cleanup;
end.
