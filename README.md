# FlashDrop: High-Concurrency Order Processing
Please see [SUBMISSION.md](file:///home/dhruv/newtonite-interview-assessment/SUBMISSION.md) for the complete submission document, architecture diagrams, deployment plan, failure scenarios, and execution instructions.

### Quick Start
```bash
# Run unit & concurrency tests (including 10,000 concurrent request simulation)
cargo test

# Run production binary
cargo run --release

# Run multi-container stack with Docker Compose (API + Redis + PostgreSQL)
docker compose up --build
```
