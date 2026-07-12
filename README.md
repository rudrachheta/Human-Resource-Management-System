# 🏢 HRMS — Human Resource Management System (C)

A console-based **Human Resource Management System** written in C, built to demonstrate core **Operating Systems / Systems Programming** concepts — process creation, inter-process communication, multithreading, synchronization, signal handling, and low-level file I/O — wrapped around a real, usable HR application.

This isn't just a CRUD app with a database; the payroll engine literally **forks a child process**, which spins up a **thread pool** to compute salaries **in parallel**, and streams results back to the parent over a **pipe**. Shortlisting candidates is done with a **chained pipeline of `grep`, `awk`, and `sort` child processes** — the same pattern the Unix shell uses for `cmd1 | cmd2 | cmd3`.

---

## ✨ Features

### Employee Management
- Add / view / search / update / delete employees
- Soft-delete: removed employees are archived (with reason + timestamp) to `ex_employees.txt` instead of being lost
- Atomic, crash-safe saves using temp file + `rename()`

### Payroll & Compensation
- Multi-threaded payroll computation (base salary + bonus % + performance bonus − deductions)
- Payslip generation to a per-employee text file
- On-demand data backup

### Performance & Attendance
- Performance score tracking per employee
- Office-hours logging and lookup
- "Scholar score" performance report pulled from a separate performance dataset

### Recruitment
- Add / view / delete applicants
- Hire an applicant → promotes them into the active employee list

### Administration
- Policies and resignation record-keeping (append-only logs)
- **Custom student/candidate shortlisting** using a live Unix pipeline (`grep -v ^$ | awk '<criteria>' | sort -k3,3nr -k5,5nr -k4,4n`), driven by user-supplied cutoff filters (CPI, min. questions solved, max. time)

### Reliability
- Graceful shutdown of the payroll worker process on exit
- Configurable input timeout (20s) that aborts a stuck prompt and safely returns to the menu instead of hanging

---

## 🧠 Systems Programming Concepts Demonstrated

This project was built specifically to put Computer Systems Programming coursework into practice. Each concept below maps directly to a piece of the codebase.

| Concept | Where it's used | Why |
|---|---|---|
| **`fork()`** | `main()` forks a dedicated **payroll child process** at startup | Isolates the CPU-bound payroll computation from the interactive UI process |
| **Anonymous pipes (`pipe()`, `dup2()`)** | Two pipes (`parent → child`, `child → parent`) connect the main process and the payroll worker | Bidirectional IPC: parent sends commands (`RUN_PAYROLL`, `EXIT`), child streams results back line-by-line |
| **Multi-process pipelines** | `shortlist_students_interactive()` forks **3 children** running `grep`, `awk`, and `sort`, wired together with two pipes and `execlp()` | Recreates shell-style piping (`grep ... \| awk ... \| sort ...`) entirely at the syscall level to filter/rank candidates |
| **POSIX Threads (`pthread_create`/`join`)** | `payroll_process_loop()` spawns one worker thread per employee inside the child process | Parallelizes payroll math across all employees instead of computing serially |
| **Semaphores (`sem_t`)** | `worker_sem`, initialized to `PAYROLL_WORKERS` (8) | Bounds how many payroll threads can run concurrently — a classic **counting semaphore / bounded resource pool** |
| **Mutex locks (`pthread_mutex_t`)** | `emp_mutex` guards the critical section of each worker | Prevents races when multiple threads compute and print/write payroll results |
| **Signal handling (`sigaction`)** | `SIGINT` (clean exit), `SIGUSR1` (trigger payroll report on demand), `SIGUSR2` (trigger backup), `SIGALRM` (input timeout) | Lets the process react asynchronously to OS-level events without polling |
| **Non-local jumps (`sigsetjmp`/`siglongjmp`)** | `timeout_buf` combined with the `SIGALRM` handler | When a user leaves a prompt idle past the timeout, execution jumps cleanly back to the menu loop instead of leaving the program blocked on `scanf` |
| **`alarm()`** | Wraps every blocking input call | Implements a **20-second input watchdog** so the program never hangs waiting on the user |
| **Low-level file I/O & atomic writes** | `mkstemp()` + `fsync()` + `rename()` in `save_employees_to_file()` | Guarantees the employee file is never left half-written if the program crashes mid-save |
| **`execlp()` / process images** | Payroll pipeline children replace themselves with `grep`/`awk`/`sort` | Demonstrates process image replacement instead of reimplementing filtering logic in C |
| **`waitpid()`** | Parent reaps the payroll child and pipeline children | Avoids zombie processes |
| **Custom CSV-based persistence** | Every module (employees, applicants, policies, resignations, ex-employees) persists to its own flat file | No external DB dependency — pure `fopen`/`fgets`/`fprintf`/`sscanf` parsing |

---

## 🏗️ Architecture

```
                 ┌────────────────────────┐
                 │        main()          │
                 │  (Parent Process / UI)  │
                 └───────────┬────────────┘
                              │ fork()
              ┌───────────────┴────────────────┐
              │                                 │
   ┌──────────▼──────────┐          ┌───────────▼────────────┐
   │  Parent: Menu Loop   │  pipe    │  Child: Payroll Engine │
   │  - Employee CRUD     │◄────────►│  - pthread pool        │
   │  - Signals (SIGINT,  │  (2 way) │  - semaphore-bounded   │
   │    SIGUSR1/2, ALRM)  │          │    concurrency         │
   │  - File persistence  │          │  - mutex-protected     │
   └──────────────────────┘          │    salary calc         │
                                      └─────────────────────────┘

   shortlist_students_interactive():
   file ──► [grep -v ^$]  ──pipe──►  [awk 'criteria']  ──pipe──►  [sort -k3nr -k5nr -k4n] ──► shortlisted.txt
              (child 1)                  (child 2)                      (child 3)
```

## 🗂️ File Layout (runtime-generated)

| File | Purpose |
|---|---|
| `employees.txt` | Active employee records (CSV) |
| `employees_backup.txt` | Manual/signal-triggered backup |
| `ex_employees.txt` | Archived employees (deleted/terminated, with reason + timestamp) |
| `applicants.txt` | Recruitment pipeline candidates |
| `policies.txt` / `resignation.txt` | Append-only admin logs |
| `employee_performance.txt` | Source data for scholar/performance reports |
| `students.txt` | Input dataset for the shortlisting pipeline |
| `shortlisted.txt` | Output of the grep/awk/sort pipeline |
| `payslip_<id>_<date>.txt` | Generated payslip per employee |

---

## 🚀 Build & Run

```bash
gcc -Wall -O2 -o hrms_full hrms_full.c -lpthread
./hrms_full
```

**Requirements:** Linux/Unix environment (uses POSIX APIs: `fork`, `pipe`, `pthread`, `sigaction`, `execlp`). `grep`, `awk`, and `sort` must be available on `PATH` for the shortlisting feature.

### Triggering signals externally
```bash
kill -USR1 <pid>   # force an on-demand payroll report
kill -USR2 <pid>   # force a backup
```

---

## 🔮 Possible Extensions

- Swap flat-file storage for SQLite while keeping the same IPC/threading architecture
- Add a `select()`/`poll()`-based multi-client server mode over sockets instead of a single local console
- Replace `sscanf`-based CSV parsing with a more robust serialization format (e.g. simple binary records)
- Add unit tests around the payroll calculation logic

---

## 📚 What This Project Demonstrates for a Systems Programming Role

This project was built to go beyond typical CRUD/web-app portfolio pieces and show hands-on comfort with:
- Process lifecycle management (`fork`/`exec`/`wait`)
- Inter-process communication via pipes
- Thread synchronization primitives (mutexes, semaphores)
- Asynchronous signal handling and safe recovery via `sigsetjmp`/`siglongjmp`
- Defensive, crash-safe file I/O

---
