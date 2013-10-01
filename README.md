#protoshell

Protoshell implements protocol buffer serialisation for Storm shell components. It implements a ProtoSerializer class, which implements the ISerializer interface.

Changes are being made to Storm which will allow this serialiser to be chosen, as an alternative to the JSON serialisation currently used.

These changes will hopefully be incorporated into Storm fairly soon, pending a pull reqeuest. If anyone requires access to these Storm features, they can use the jsgilmore Storm fork.
