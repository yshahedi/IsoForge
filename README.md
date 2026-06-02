# IsoForge

IsoForge is a high-performance C++ engine designed for processing ISO8583 financial messages. Built for extreme throughput and low latency, IsoForge is engineered to handle massive workloads in demanding financial environments.

## Key Performance Metrics
*   **Throughput:** 1,000,000+ TPS (Transactions Per Second).
*   **Concurrency:** Supports 10,000+ concurrent connections.
*   **Latency:** Ultra-low latency processing with intelligent timeout management.

## Features
- **High Concurrency:** Optimized for handling thousands of persistent connections using modern C++ asynchronous patterns.
- **Robust Timeout Handling:** Intelligent mechanism to drop stale connections and manage transaction lifecycles efficiently.
- **Performance Optimized:** Minimal memory footprint and lock-free data structures to maximize CPU throughput.
- **Protocol Compliant:** Reliable ISO8583 message parsing and serialization.

## Tech Stack
IsoForge leverages high-performance libraries to achieve its benchmarks:
*   **Networking:** [Asio](https://think-async.com/Asio/) (Asynchronous I/O)
*   **Parsing/Processing:** [Oscar Sanderson's Library] (https://oscarsanderson.com/iso-8583/)

## Getting Started

### Prerequisites
- C++17 or higher
- A modern C++ compiler (GCC 9+, Clang 10+, or MSVC 2019+)
