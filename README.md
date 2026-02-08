# wheelchoke-system-communication-system
A robust, type-safe IPC (Inter-Process Communication) library designed for sensor networks. It provides a unified interface for ZeroMQ (and future MQTT) with Pydantic validation, supporting both synchronous and asynchronous (asyncio) operations.

## 📂 Project Structure

Here is a brief overview of the core files in this library:

```text
src/msg_handler/
├── __init__.py                # Exposes public APIs and version information for easy import.
├── schemas.py                 # Defines data models and validation rules using Pydantic.
├── pub_base.py                # Abstract base class defining the interface and context manager for publishers.
├── pub_factory.py             # Factory function to instantiate the appropriate publisher based on configuration.
├── sub_base.py                # Abstract base classes defining interfaces for synchronous and asynchronous subscribers.
├── sub_factory.py             # Factory function to instantiate the appropriate subscriber based on configuration.
└── backends/
    ├── pub_zmq.py             # ZeroMQ implementation of the publisher with support for bind/connect modes.
    └── sub_zmq.py             # ZeroMQ implementation of synchronous and asynchronous subscribers
```

## Features
- Type Safety: strictly validated messages using Pydantic.

- Dual Mode: Supports both Synchronous (threading) and Asynchronous (asyncio) execution.

- Flexible Topology: Easily configurable bind (Server-like) and connect (Client-like) modes.

- Resource Management: Automatic socket cleanup via Python Context Managers (with / async with).


## 🚀 Installation

This project uses **Poetry** for dependency management.

```bash
# Install dependencies
poetry install
```

# Usage

import msg_handler should be like this
```
  poetry add git+https://github.com/TRU-Capstone-2026-WheelChock/wheelchoke-system-communication-system.git
```
 plz see test_msg_handler_one_process.py for examples