#protoshell

Protoshell implements a protocol buffer serialiaser for Storm's multi-language
support that greatly improves Storm's multilang throughput over the build-in
JSON serialiser.

Protoshell requies that all messages sent and received as part of the tuple
fields be protocol buffer messages.

Protoshell is a binary communications protocol, as opposed to the text-based
communications protocol used in Storm's JSON serialiser. It transfers varint
delimited bytes, where each byte slice is a serialised protocol buffer structure.

Please refer to [GoStorm](https://github.com/jsgilmore/gostorm) and its protobuf
encoding for a reference implementation of a third-party non-JVM language implementation
of the Storm multilang protocol written in Go and requiring the protoshell serialiser.

Storm 0.9.2 and later now support pluggable multilang serialisation, based on this protobuf work, which means that protoshell can now be used with standard Storm.

#Performance
Protoshell currently provides three to fives times higher throughput, when compared to the standard Storm JSON multilang protocol.

#The protocol
To simplify the process of implementing third-party components in other languges,
the protoshell serialiser to a large degree follows the same structure as the
Storm multilang protocol.

As per the multilang protocol, a third-party implementation would first have to
receive the topology context. The Context object is specified in messages.proto.

The third-party implementation must then respond with its process id (pid), using
the Pid object in messages.proto.

The third-party application should also write its pid to the folder specified in
the PidDir field in the Context object. Storm uses this pid to kill a components
when an error occurs or the topology is killed.

After the pid has been communicated and recorded, the component can start to
receive message.

A spout will receive SpoutMsgProto's and a bolt will receive BoltMsgProto's.
The same rules that govern the reception of messages in Storm's multilang protocol
also apply to the protoshell communications protocol.

If a third-party implementation wishes to transfer data to storm, it should do
so by making use of the ShellMsgProto object.
