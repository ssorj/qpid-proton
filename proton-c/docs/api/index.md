Proton Documentation            {#index}
====================

## The Protocol Engine

The [Engine API](@ref engine) is a "pure AMQP" toolkit, it decodes AMQP bytes
into proton [events](@ref event) and generates AMQP bytes from application
calls.

The [connection engine](@ref connection_engine) provides a simple bytes in/bytes
out, event-driven interface so you can read AMQP data from any source, process
the resulting [events](@ref event) and write AMQP output to any destination.

There is no IO or threading code in this part of the library, so it can be
embedded in many different environments. The proton project provides language
bindings (Python, Ruby, Go etc.) that embed it into the standard IO and
threading facilities of the bound language.

## Integrating with IO

The [Driver SPI](@ref driver) is a portable framework to build single or
multi-threaded Proton C applications with replaceable IO implementations.

The driver can initiate or listen for connections. Application threads wait for
a [connection engine](@ref connection_engine) to become ready for processing due
to an IO event. The application thread uses [Engine API](@ref engine) described
above, fully decoupled from the IO mechanism.

The driver is the basis for the Proton C++ binding and the
[Qpid Disptch Router](http://qpid.apache.org/components/dispatch-router/) The
Proton project provides drivers for many platforms, and you can implement your
own.

## Messenger and Reactor APIs

The [Messenger](@ref messenger) [Reactor](@ref reactor) APIs were intended
to be simple APIs that included IO support directly out of the box.

They both had good points but were both based on POSIX-style polling
assumptions, and did not support concurrent or multi-threaded use. This creates
several problems:

- Difficult to port (e.g. Windows poll() is inefficient, IOCP has a different model)
- Difficult to integrate: even in python and ruby, foreign C code blocking on IO violates normal IO and threading assumptions and requires some dubious hacking to make it work.
- Impossible to use in multi-threaded servers or concurrent languages like Go.

Note however that the Reactor API was built around the Proton @ref engine and
@ref event APIs. For the most part using the @ref driver or @ref connection_engine
is the same as using the reactor. Connection setup/teardown and pumping the
event loop are the main differences.
