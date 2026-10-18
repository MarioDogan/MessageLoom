You are developing C++20 code for a message passing framework called MessageLoom.
Namespace of framework should be mloom.

Here is the framework arhitecture overview.
Nodes are deployiment units that have in/out data sockets and one control and one status socket.
MessageQueue is an entity that provides pub and sub sockets and abstracts the communication mechanism.
Sockets are abstractions of data channels, they are either pub or sub, but can be inproc, udp, tcp etc...
NodeCore is being injected into Node, and in Node Core, Node Loop and and socket manager are injected.
NodeLoop defines logic how user bussines code will be called e.g. FreeRunningSourceLoop does not need external triggering to start and itself does not inputs, FreeRunningLoop has input and outputs and reacts on receiveing and input data, we can alos have Triggerd versions of loops, they block on control socket and only do bussines logic after receiving the control signal.
FreeRunningLoop will not block on control signal, it will check if it exists if it does will do state switch if needed and then go to run bussines logic.
In this way we can have purerly event driven system or one that is being run by external orchestrator.
LogicHandler are types that are used for bussines logic integration (3rd party code). 3rd party code just needs to register itself to a logic handler which will then be injected to a node loop....
The logic handlers are of type std::pair<retval, mloom::status>=f(const& in type), or std::pair<retval, mloom::status>=f( in type&& ), depending if in type can be completly consumed (mostly meaning stuff can be moved out it)

In status type can return need more data, error, or ok,etc...
Messages being passes between nodes, are fundamentally do type erasure and for inproc they are shared_ptr<Message> being passed by messages queue and sockets, it is up to logic handlers to then know how to extract type information back. Fundamentally it is an expectation that connected nodes do know on which socket they are receiving which type which then allows logic handelr to call Message.IsType<handler_type>. Message.IsType should use e.g hash values for checking types and typeid for simple check.

I want bazel to be used as a build sistem. And first implementation should be based on ZeroMq. Meaning MessageQueue and Sockets need to abstarct ZeroMq.
Note I do not wanna any kind of port style systems.

When we arhitecture done up to Node level, then we have to do node orchestration and stageing where we have node runners and stageing part how the nodes are connected etc...