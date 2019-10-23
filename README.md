# mosquitto-p

Free Pascal conversions of the libmosquitto header file `mosquitto.h`,
as part of the Eclipse Mosquitto project, and some Pascal examples
and language integration.

This allows using the libmosquitto MQTT client library, part of the
Mosquitto MQTT Broker project from Free Pascal applications.

For the original sources, see:
https://github.com/eclipse/mosquitto

### Source Files

* `mosquitto.pas` - conversion of the C `mosquitto.h` header to Pascal,
                    provides the same API as the C version
* `mqttclass.pas` - Object Pascal wrapper class to ease the integration
                    of libmosquitto into Object Oriented applications
* `test.pas`      - Test code for the Pascal header unit with C-like API
* `testclass.pas` - Test code for the Object Pascal wrapper class

### mqttclass.pas

It is a fully featured Object Pascal class to handle MQTT connections via
libmosquitto. Apart from providing an OOP-style API, it maps the low-level
C types used by libmosquitto itself to higher level Pascal types. For
example, you can pass a `String` instead of a `PChar/char *` everywhere.
Thanks to Free Pascal's native threading features it can be fully
asynchronous and behave equally on all platforms. This feature also works
on Windows, without depending on pthreads on this platform, unlike
libmosquitto itself.

### License

The Eclipse Mosquitto project is licensed under the Eclipse Public License 1.0.

The contents of this repository are covered by the ISC License, see the
`LICENSE` file for details.
