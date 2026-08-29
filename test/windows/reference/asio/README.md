# ASIO Reference Behavior Probes

These programs are compiled against the full ASIO headers under
`src/kernel_win/asio/include`. They are the behavior reference for the
Windows async kernel.

Each probe prints observable ASIO behavior that the corresponding AsyncCore
module must reproduce:

- callback order;
- handler invocation counts;
- error codes;
- cancellation results;
- timer expiry/cancel/reuse;
- strand serialization;
- work accounting.

Do not add AsyncCore debug tests here. This directory is for collecting ASIO
behavior data only.
