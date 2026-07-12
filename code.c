/*
 * hrms_full.c
 *
 * Full-feature HRMS console application in C for a Computer Systems Programming project.
 * - Uses fork+pipe for a payroll child process
 * - Child uses pthreads + semaphores to compute payroll concurrently
 * - Parent provides interactive menu for HR features: employees, payroll, performance,
 *   office hours, recruitment, training, promotion, resignation, termination, assets, jobs, policies
 * - Persistent file storage for each module (simple CSV-like)
 * - Signal handling: SIGINT (exit), SIGUSR1 (trigger payroll report), SIGUSR2 (backup)
 * - NEW: SIGALRM for 10-second input timeout warning
 *
 * Build:
 *   gcc -Wall -O2 -o hrms_full hrms_full.c -lpthread
 *
 * Run:
 *   ./hrms_full
 */

#define _XOPEN_SOURCE 700
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <semaphore.h>
#include <sys/stat.h>
#include <time.h>

#define RESET "\033[0m"
#define BOLD "\033[1m"
#define RED "\033[31m"
#define GREEN "\033[32m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN "\033[36m"
#define BRIGHT_CYAN "\033[96m"
#define BRIGHT_GREEN "\033[92m"
#define BRIGHT_WHITE "\033[97m"
#define BG_BLUE "\033[44m"
#define WHITE "\033[37m"

void clr() { printf("\033[2J\033[H"); }
void hdr(const char *t) { 
    printf("\n" BOLD CYAN "╔══════════════════════════════════════════════════════════════════════╗\n" RESET);
    printf(BOLD CYAN "║" RESET); 
    int p = (70 - strlen(t)) / 2;
    for(int i=0; i<p; i++) printf(" ");
    printf(BOLD BRIGHT_WHITE "%s" RESET, t);
    for(int i=0; i<70-p-strlen(t); i++) printf(" ");
    printf(BOLD CYAN "║\n╚══════════════════════════════════════════════════════════════════════╝\n" RESET);
}
void sep() { printf(CYAN "──────────────────────────────────────────────────────────────────────\n" RESET); }
void succ(const char *m) { printf(BOLD GREEN "✓ %s\n" RESET, m); }
void err(const char *m) { printf(BOLD RED "✗ %s\n" RESET, m); }
void warn(const char *m) { printf(BOLD YELLOW "⚠ %s\n" RESET, m); }
void info(const char *m) { printf(BOLD BLUE "ℹ %s\n" RESET, m); }
void mi(int n, const char *i) { printf(BRIGHT_CYAN "  • " RESET BRIGHT_WHITE "%d" RESET ") " YELLOW "%s\n" RESET, n, i); }
void pr(const char *m) { printf(BOLD MAGENTA "%s → " RESET, m); }
void pec() { printf("\nPress ENTER..."); while(getchar()!='\n'); getchar(); }

#define EMP_FILE "employees.txt"
#define BACKUP_FILE "employees_backup.txt"
#define EX_EMP_FILE "ex_employees.txt"
#define APPLICANT_FILE "applicants.txt"
#define POLICIES_FILE "policies.txt"
#define RESIGNATION_FILE "resignation.txt"
#define STUDENT_INPUT_DEFAULT "students.txt"
#define SHORTLISTED_FILE "shortlisted.txt"

#define MAX_EMP 512
#define NAME_LEN 64
#define BUFFER_SIZE 512
#define PAYROLL_WORKERS 8
#define INPUT_TIMEOUT 20

/* synchronization for payroll workers (in child) */
static pthread_mutex_t emp_mutex = PTHREAD_MUTEX_INITIALIZER;
static sem_t worker_sem;

/* Employee structure */
typedef struct Employee {
    int id;
    char name[NAME_LEN];
    char department[32];
    double base_salary;
    double bonus_percent;
    double deduction;
    double performance_score; /* 0..100 */
    double total_hours;       /* e.g., monthly */
} Employee;

/* In-memory employee list (parent) */
static Employee emp_list[MAX_EMP];
static int emp_count = 0;

/* Signals */
static volatile sig_atomic_t sig_exit = 0;
static volatile sig_atomic_t sig_report = 0;
static volatile sig_atomic_t sig_backup = 0;
static volatile sig_atomic_t sig_timeout = 0;

/* Timeout handling with longjmp */
#include <setjmp.h>
static sigjmp_buf timeout_buf;
static volatile sig_atomic_t timeout_enabled = 0;

/* Timeout handler function - terminates current process and returns to menu */
void timeout_handler(void) {
    alarm(0);  /* Cancel any pending alarm */
    timeout_enabled = 0;
    printf("\n\n*** TIMEOUT ERROR: No input received for 10 seconds! ***\n");
    printf("*** Terminating current operation and returning to menu... ***\n\n");
    
    /* Clear input buffer */
    int c;
    while ((c = getchar()) != '\n' && c != EOF);
    
    /* Jump back to saved point (menu) */
    if (timeout_enabled) {
        siglongjmp(timeout_buf, 1);
    }
}

void fatal(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}

/* Parent signal handler */
void parent_signal_handler(int sig) {
    if (sig == SIGINT) sig_exit = 1;
    else if (sig == SIGUSR1) sig_report = 1;
    else if (sig == SIGUSR2) sig_backup = 1;
    else if (sig == SIGALRM) {
        sig_timeout = 1;
        if (timeout_enabled) {
            alarm(0);
            timeout_enabled = 0;
            printf("\n\n*** TIMEOUT ERROR: No input received for 10 seconds! ***\n");
            printf("*** Terminating current operation... ***\n");
            printf("*** Press ENTER to go back to main menu ***\n");
            siglongjmp(timeout_buf, 1);
        }
    }
}

/* Utility: safe file copy (backup) */
void backup_file(const char *src, const char *dst) {
    FILE *fsrc = fopen(src, "r");
    if (!fsrc) {
        perror("backup fopen src");
        return;
    }
    FILE *fdst = fopen(dst, "w");
    if (!fdst) {
        perror("backup fopen dst");
        fclose(fsrc);
        return;
    }
    char buf[1024];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fsrc)) > 0) {
        if (fwrite(buf, 1, n, fdst) != n) {
            perror("backup fwrite");
            break;
        }
    }
    fclose(fsrc);
    fclose(fdst);
    printf("Backup completed: %s -> %s\n", src, dst);
}

void getScholarScore() {
    FILE *fp = fopen("employee_performance.txt", "r");
    if (!fp) {
        printf("Error: Cannot open employee_performance.txt\n");
        return;
    }

    int search_id;
    printf("Enter Employee ID: ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%d", &search_id) != 1) {
        alarm(0);
        while(getchar()!='\n');
        printf("Invalid input\n");
        fclose(fp);
        return;
    }
    alarm(0);

    char line[200];
    int found = 0;

    // Skip header line
    if (!fgets(line, sizeof(line), fp)) {
        fclose(fp);
        return;
    }

    while (fgets(line, sizeof(line), fp)) {
        int id, present, tasks;
        char name[50], dept[30];
        double salary, productivity, attendance, performance;

        // Parse the row
        sscanf(line, "%d\t%49[^\t]\t%29[^\t]\t%lf\t%d\t%d\t%lf\t%lf\t%lf",
               &id, name, dept, &salary, &present, &tasks,
               &productivity, &attendance, &performance);

        if (id == search_id) {
            found = 1;
            int scholar = (int)(performance * 100);

            printf("\n===== SCHOLAR PERFORMANCE REPORT =====\n");
            printf("Employee ID   : %d\n", id);
            printf("Name          : %s\n", name);
            printf("Department    : %s\n", dept);
            printf("Scholar Score : %d / 100\n", scholar);
            printf("======================================\n");
            break;
        }
    }

    if (!found) {
        printf("Employee ID %d not found in performance file.\n", search_id);
    }

    fclose(fp);
}

/* ================= File I/O: employees ================= */

void load_employees_from_file(void) {
    FILE *f = fopen(EMP_FILE, "r");
    if (!f) {
        if (errno == ENOENT) {
            emp_count = 0;
            return;
        }
        perror("fopen load_employees");
        return;
    }
    emp_count = 0;
    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        if (emp_count >= MAX_EMP) break;
        line[strcspn(line, "\n")] = '\0';
        Employee e;
        e.performance_score = 0.0;
        e.total_hours = 0.0;
        int matched = sscanf(line, "%d,%63[^,],%31[^,],%lf,%lf,%lf,%lf,%lf",
                             &e.id, e.name, e.department,
                             &e.base_salary, &e.bonus_percent, &e.deduction,
                             &e.performance_score, &e.total_hours);
        if (matched == 8) {
            emp_list[emp_count++] = e;
            continue;
        }
        matched = sscanf(line, "%d,%63[^,],%31[^,],%lf,%lf,%lf",
                         &e.id, e.name, e.department,
                         &e.base_salary, &e.bonus_percent, &e.deduction);
        if (matched == 6) {
            e.performance_score = 0.0;
            e.total_hours = 0.0;
            emp_list[emp_count++] = e;
            continue;
        }
    }
    fclose(f);
}

void save_employees_to_file(void) {
    char tmpfile[] = "employees.tmp.XXXXXX";
    int fd = mkstemp(tmpfile);
    if (fd == -1) {
        perror("mkstemp");
        return;
    }
    FILE *f = fdopen(fd, "w");
    if (!f) {
        perror("fdopen");
        close(fd);
        unlink(tmpfile);
        return;
    }
    for (int i = 0; i < emp_count; ++i) {
        fprintf(f, "%d,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f\n",
                emp_list[i].id,
                emp_list[i].name,
                emp_list[i].department,
                emp_list[i].base_salary,
                emp_list[i].bonus_percent,
                emp_list[i].deduction,
                emp_list[i].performance_score,
                emp_list[i].total_hours);
    }
    fflush(f);
    fsync(fd);
    fclose(f);
    if (rename(tmpfile, EMP_FILE) != 0) {
        perror("rename temp to emp file");
        unlink(tmpfile);
    }
}

void append_to_ex_employee_file(const Employee *e, const char *reason) {
    if (!e) return;
    FILE *f = fopen(EX_EMP_FILE, "a");
    if (!f) {
        perror("open ex_employee file");
        return;
    }
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);
    char datestr[64];
    snprintf(datestr, sizeof(datestr), "%04d-%02d-%02d %02d:%02d:%02d",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec);
    char safe_reason[256];
    if (reason && strlen(reason) > 0) {
        strncpy(safe_reason, reason, sizeof(safe_reason)-1);
        safe_reason[sizeof(safe_reason)-1] = '\0';
    } else {
        safe_reason[0] = '\0';
    }
    fprintf(f, "%d,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%s,%s\n",
            e->id, e->name, e->department,
            e->base_salary, e->bonus_percent, e->deduction,
            e->performance_score, e->total_hours,
            datestr, safe_reason);
    fclose(f);
}

/* ================= Helpers for CRUD ================= */

int find_employee_index_by_id(int id) {
    for (int i = 0; i < emp_count; ++i) if (emp_list[i].id == id) return i;
    return -1;
}

void add_employee_interactive(void) {
    if (sigsetjmp(timeout_buf, 1) != 0) {
        /* Timeout occurred - wait for user to press enter, then return to menu */
        alarm(0);
        timeout_enabled = 0;
        getchar(); /* Wait for ENTER key */
        return;
    }
    
    if (emp_count >= MAX_EMP) { printf("Employee list is full!\n"); return; }
    Employee e;
    memset(&e, 0, sizeof(e));
    
    printf("Enter ID: ");
    timeout_enabled = 1;
    alarm(INPUT_TIMEOUT);
    if (scanf("%d", &e.id) != 1) { alarm(0); timeout_enabled = 0; while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    timeout_enabled = 0;
    
    if (find_employee_index_by_id(e.id) != -1) { printf("ID already exists.\n"); return; }
    
    printf("Enter Name: "); 
    while(getchar()!='\n');
    timeout_enabled = 1;
    alarm(INPUT_TIMEOUT);
    if (!fgets(e.name, NAME_LEN, stdin)) { alarm(0); timeout_enabled = 0; return; }
    alarm(0);
    timeout_enabled = 0;
    e.name[strcspn(e.name, "\n")] = '\0';
    
    printf("Enter Department: ");
    timeout_enabled = 1;
    alarm(INPUT_TIMEOUT);
    if (!fgets(e.department, sizeof(e.department), stdin)) { alarm(0); timeout_enabled = 0; return; }
    alarm(0);
    timeout_enabled = 0;
    e.department[strcspn(e.department, "\n")] = '\0';
    
    printf("Enter Base Salary: ");
    timeout_enabled = 1;
    alarm(INPUT_TIMEOUT);
    if (scanf("%lf", &e.base_salary) != 1) { alarm(0); timeout_enabled = 0; while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    timeout_enabled = 0;
    
    printf("Enter Bonus %% (e.g. 10 for 10%%): ");
    timeout_enabled = 1;
    alarm(INPUT_TIMEOUT);
    if (scanf("%lf", &e.bonus_percent) != 1) { alarm(0); timeout_enabled = 0; while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    timeout_enabled = 0;
    
    printf("Enter Deductions (absolute): ");
    timeout_enabled = 1;
    alarm(INPUT_TIMEOUT);
    if (scanf("%lf", &e.deduction) != 1) { alarm(0); timeout_enabled = 0; while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    timeout_enabled = 0;
    
    e.performance_score = 0.0; e.total_hours = 0.0;
    emp_list[emp_count++] = e;
    save_employees_to_file();
    printf("Employee added and saved.\n");
}

void view_all_employees(void) {
    load_employees_from_file();
    if (emp_count == 0) { printf("No employees found.\n"); return; }
    printf("%-6s %-20s %-12s %-10s %-7s %-10s %-6s %-6s\n", "ID", "Name", "Dept", "Base", "Bonus%", "Deduct", "Perf", "Hours");
    for (int i = 0; i < emp_count; ++i) {
        printf("%-6d %-20s %-12s %-10.2f %-7.2f %-10.2f %-6.2f %-6.2f\n",
               emp_list[i].id, emp_list[i].name, emp_list[i].department,
               emp_list[i].base_salary, emp_list[i].bonus_percent,
               emp_list[i].deduction, emp_list[i].performance_score, emp_list[i].total_hours);
    }
}

void search_employee(void) {
    if (sigsetjmp(timeout_buf, 1) != 0) {
        /* Timeout occurred - wait for ENTER then return */
        alarm(0);
        timeout_enabled = 0;
        getchar(); /* Wait for ENTER key */
        return;
    }
    
    int id;
    printf("Enter ID to search: ");
    timeout_enabled = 1;
    alarm(INPUT_TIMEOUT);
    if (scanf("%d", &id) != 1) { alarm(0); timeout_enabled = 0; while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    timeout_enabled = 0;
    
    load_employees_from_file();
    int idx = find_employee_index_by_id(id);
    if (idx == -1) { printf("Employee not found.\n"); return; }
    Employee *e = &emp_list[idx];
    printf("Found: ID=%d, Name=%s, Dept=%s, Base=%.2f, Bonus=%.2f%%, Deduction=%.2f, Perf=%.2f, Hours=%.2f\n",
           e->id, e->name, e->department, e->base_salary, e->bonus_percent, e->deduction, e->performance_score, e->total_hours);
}

void delete_employee(void) {
    if (sigsetjmp(timeout_buf, 1) != 0) {
        /* Timeout occurred - wait for ENTER then return */
        alarm(0);
        timeout_enabled = 0;
        getchar(); /* Wait for ENTER key */
        return;
    }
    
    int id;
    printf("Enter ID to delete: ");
    timeout_enabled = 1;
    alarm(INPUT_TIMEOUT);
    if (scanf("%d", &id) != 1) { alarm(0); timeout_enabled = 0; while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    timeout_enabled = 0;
    
    load_employees_from_file();
    int idx = find_employee_index_by_id(id);
    if (idx == -1) { printf("Employee not found.\n"); return; }

    Employee removed = emp_list[idx];
    printf("About to delete: ID=%d, Name=%s, Dept=%s, Base=%.2f\n", removed.id, removed.name, removed.department, removed.base_salary);

    char reason[256];
    printf("Enter reason for leaving/termination (optional, single line): ");
    while(getchar()!='\n');
    timeout_enabled = 1;
    alarm(INPUT_TIMEOUT);
    if (!fgets(reason, sizeof(reason), stdin)) reason[0] = '\0';
    alarm(0);
    timeout_enabled = 0;
    reason[strcspn(reason, "\n")] = '\0';

    append_to_ex_employee_file(&removed, reason);

    for (int i = idx; i < emp_count - 1; ++i) emp_list[i] = emp_list[i+1];
    emp_count--;
    save_employees_to_file();
    printf("Employee moved to %s and removed from active list.\n", EX_EMP_FILE);
}

void update_employee(void) {
    int id;
    printf("Enter ID to update: ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%d", &id) != 1) { alarm(0); while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    
    load_employees_from_file();
    int idx = find_employee_index_by_id(id);
    if (idx == -1) { printf("Employee not found.\n"); return; }
    Employee *e = &emp_list[idx];
    printf("Updating employee %d (%s)\n", e->id, e->name);
    while(getchar()!='\n');
    char buf[NAME_LEN];
    
    printf("New Name (blank to keep): ");
    alarm(INPUT_TIMEOUT);
    if (fgets(buf, sizeof(buf), stdin)) { 
        alarm(0); 
        buf[strcspn(buf, "\n")] = '\0'; 
        if (strlen(buf) > 0) {
            size_t len = strlen(buf);
            if (len >= NAME_LEN) len = NAME_LEN - 1;
            memcpy(e->name, buf, len);
            e->name[len] = '\0';
        }
    }
    else alarm(0);
    
    printf("New Dept (blank to keep): ");
    alarm(INPUT_TIMEOUT);
    if (fgets(buf, sizeof(buf), stdin)) { 
        alarm(0); 
        buf[strcspn(buf, "\n")] = '\0'; 
        if (strlen(buf) > 0) {
            size_t len = strlen(buf);
            if (len >= sizeof(e->department)) len = sizeof(e->department) - 1;
            memcpy(e->department, buf, len);
            e->department[len] = '\0';
        }
    }
    else alarm(0);
    
    printf("New Base Salary (-1 to keep): ");
    double d;
    alarm(INPUT_TIMEOUT);
    if (scanf("%lf", &d)==1) { alarm(0); if (d >= 0) e->base_salary = d; }
    else alarm(0);
    
    printf("New Bonus %% (-1 to keep): ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%lf", &d)==1) { alarm(0); if (d >= 0) e->bonus_percent = d; }
    else alarm(0);
    
    printf("New Deductions (-1 to keep): ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%lf", &d)==1) { alarm(0); if (d >= 0) e->deduction = d; }
    else alarm(0);
    
    printf("New Performance (-1 to keep): ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%lf", &d)==1) { alarm(0); if (d >= 0) e->performance_score = d; }
    else alarm(0);
    
    printf("New Total Hours (-1 to keep): ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%lf", &d)==1) { alarm(0); if (d >= 0) e->total_hours = d; }
    else alarm(0);
    
    save_employees_to_file();
    printf("Updated and saved.\n");
}

/* ================= Payroll (child process) ================= */

typedef struct PayrollArg {
    Employee emp;
    int out_fd;
} PayrollArg;

void *payroll_worker(void *arg) {
    PayrollArg *p = (PayrollArg *)arg;
    if (!p) return NULL;

    sem_wait(&worker_sem);
    pthread_mutex_lock(&emp_mutex);

    double perf_bonus = (p->emp.performance_score / 100.0) * 500.0;
    double gross = p->emp.base_salary + (p->emp.base_salary * p->emp.bonus_percent / 100.0) + perf_bonus;
    double net = gross - p->emp.deduction;

    for (volatile int i = 0; i < 1000000; ++i);

    char outbuf[512];
    int n = snprintf(outbuf, sizeof(outbuf), "PAYROLL_RESULT,%d,%s,%.2f\n", p->emp.id, p->emp.name, net);

    if (p->out_fd >= 0) {
        ssize_t w = write(p->out_fd, outbuf, n);
        if (w == -1) perror("worker write to pipe");
    } else {
        printf("[Worker] %s: Net = %.2f\n", p->emp.name, net);
    }

    pthread_mutex_unlock(&emp_mutex);
    sem_post(&worker_sem);
    free(p);
    return NULL;
}

void payroll_process_loop(int pipe_read_fd, int pipe_write_fd) {
    sem_init(&worker_sem, 0, PAYROLL_WORKERS);
    pthread_mutex_init(&emp_mutex, NULL);

    char buf[BUFFER_SIZE];
    while (1) {
        ssize_t r = read(pipe_read_fd, buf, sizeof(buf)-1);
        if (r == 0) break;
        if (r < 0) {
            if (errno == EINTR) continue;
            perror("payroll read");
            break;
        }
        buf[r] = '\0';
        buf[strcspn(buf, "\n")] = '\0';

        if (strcmp(buf, "EXIT") == 0) {
            printf("Payroll child: EXIT received\n");
            break;
        } else if (strcmp(buf, "RUN_PAYROLL") == 0) {
            Employee local[MAX_EMP];
            int local_count = 0;
            FILE *f = fopen(EMP_FILE, "r");
            if (!f) {
                perror("payroll fopen emp file");
                continue;
            }
            char line[1024];
            while (fgets(line, sizeof(line), f) && local_count < MAX_EMP) {
                line[strcspn(line, "\n")] = '\0';
                Employee e;
                e.performance_score = 0.0; e.total_hours = 0.0;
                int matched = sscanf(line, "%d,%63[^,],%31[^,],%lf,%lf,%lf,%lf,%lf",
                                     &e.id, e.name, e.department,
                                     &e.base_salary, &e.bonus_percent, &e.deduction,
                                     &e.performance_score, &e.total_hours);
                if (matched >= 6) {
                    local[local_count++] = e;
                }
            }
            fclose(f);

            pthread_t tids[local_count > 0 ? local_count : 1];
            int created = 0;
            for (int i = 0; i < local_count; ++i) {
                PayrollArg *arg = malloc(sizeof(PayrollArg));
                if (!arg) { perror("malloc"); break; }
                arg->emp = local[i];
                arg->out_fd = pipe_write_fd;
                if (pthread_create(&tids[created], NULL, payroll_worker, arg) != 0) {
                    perror("pthread_create");
                    free(arg);
                    continue;
                }
                created++;
            }
            for (int i = 0; i < created; ++i) pthread_join(tids[i], NULL);

            if (pipe_write_fd >= 0) {
                const char *done = "PAYROLL_DONE\n";
                ssize_t w = write(pipe_write_fd, done, strlen(done));
                if (w == -1) perror("write done");
            }
        } else {
            if (pipe_write_fd >= 0) {
                char rmsg[256];
                int n = snprintf(rmsg, sizeof(rmsg), "UNKNOWN_CMD,%s\n", buf);
                ssize_t w = write(pipe_write_fd, rmsg, n);
                if (w == -1) perror("write unknown cmd");
            }
        }
    }

    pthread_mutex_destroy(&emp_mutex);
    sem_destroy(&worker_sem);
    if (pipe_read_fd >= 0) close(pipe_read_fd);
    if (pipe_write_fd >= 0) close(pipe_write_fd);
}

/* ================= Recruitment ================= */

typedef struct Applicant {
    int id;
    char name[NAME_LEN];
    char position[32];
    double expected_salary;
} Applicant;

void add_applicant() {
    Applicant a;
    printf("Enter Applicant ID: ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%d",&a.id) != 1) { alarm(0); while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    
    printf("Enter Name: ");
    while(getchar()!='\n');
    alarm(INPUT_TIMEOUT);
    if (!fgets(a.name, NAME_LEN, stdin)) { alarm(0); return; }
    alarm(0);
    a.name[strcspn(a.name,"\n")] = '\0';
    
    printf("Enter Position: ");
    alarm(INPUT_TIMEOUT);
    if (!fgets(a.position, sizeof(a.position), stdin)) { alarm(0); return; }
    alarm(0);
    a.position[strcspn(a.position,"\n")] = '\0';
    
    printf("Enter Expected Salary: ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%lf",&a.expected_salary) != 1) { alarm(0); while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    
    FILE *f = fopen(APPLICANT_FILE, "a");
    if (!f) { perror("open applicants"); return; }
    fprintf(f,"%d,%s,%s,%.2f\n", a.id, a.name, a.position, a.expected_salary);
    fclose(f);
    printf("Applicant added.\n");
}

void view_applicants() {
    FILE *f = fopen(APPLICANT_FILE, "r");
    if (!f) { printf("No applicants found.\n"); return; }
    printf("Applicants:\n");
    char line[512];
    while (fgets(line, sizeof(line), f)) {
        Applicant a;
        if (sscanf(line, "%d,%63[^,],%31[^,],%lf", &a.id, a.name, a.position, &a.expected_salary) >= 3) {
            printf("- ID:%d Name:%s Position:%s Salary:%.2f\n", a.id, a.name, a.position, a.expected_salary);
        }
    }
    fclose(f);
}

void hire_applicant() {
    int id;
    printf("Enter Applicant ID to hire: ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%d", &id) != 1) { alarm(0); while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    
    FILE *fin = fopen(APPLICANT_FILE, "r");
    if (!fin) { printf("No applicants file.\n"); return; }
    FILE *ftmp = fopen("applicants.tmp", "w");
    if (!ftmp) { perror("open temp"); fclose(fin); return; }
    char line[512]; Applicant a; int found = 0;
    load_employees_from_file();
    while (fgets(line, sizeof(line), fin)) {
        if (sscanf(line, "%d,%63[^,],%31[^,],%lf", &a.id, a.name, a.position, &a.expected_salary) >= 3) {
            if (a.id == id) {
                found = 1;
                if (emp_count >= MAX_EMP) {
                    printf("Employee list full; cannot hire.\n");
                } else {
                    Employee e; memset(&e,0,sizeof(e));
                    e.id = a.id; strncpy(e.name, a.name, NAME_LEN); strncpy(e.department, a.position, sizeof(e.department));
                    e.base_salary = a.expected_salary; e.bonus_percent = 10.0; e.deduction = 0.0; e.performance_score = 0.0; e.total_hours = 0.0;
                    emp_list[emp_count++] = e;
                }
            } else {
                fputs(line, ftmp);
            }
        } else {
            fputs(line, ftmp);
        }
    }
    fclose(fin); fclose(ftmp);
    rename("applicants.tmp", APPLICANT_FILE);
    if (found) {
        save_employees_to_file();
        printf("Hired and added to employees.\n");
    } else {
        printf("Applicant ID not found.\n");
    }
}

void delete_applicant() {
    int id;
    printf("Enter Applicant ID to delete: ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%d", &id) != 1) { alarm(0); while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    
    FILE *fin = fopen(APPLICANT_FILE, "r");
    if (!fin) { printf("No applicants.\n"); return; }
    FILE *ftmp = fopen("applicants.tmp", "w");
    char line[512]; Applicant a; int found = 0;
    while (fgets(line, sizeof(line), fin)) {
        if (sscanf(line, "%d,%63[^,],%31[^,],%lf", &a.id, a.name, a.position, &a.expected_salary) >= 3) {
            if (a.id == id) { found = 1; continue; }
        }
        fputs(line, ftmp);
    }
    fclose(fin); fclose(ftmp);
    rename("applicants.tmp", APPLICANT_FILE);
    if (found) printf("Applicant removed.\n"); else printf("Applicant not found.\n");
}

/* ================= Simple module helpers ================= */

void add_record_to_file(const char *file, const char *record) {
    FILE *f = fopen(file, "a");
    if (!f) { perror("open file"); return; }
    fprintf(f, "%s\n", record);
    fclose(f);
}

void view_records(const char *file) {
    FILE *f = fopen(file, "r");
    if (!f) { printf("No records for %s\n", file); return; }
    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        printf("%s", line);
    }
    fclose(f);
}

/* ================= Payslip generation ================= */

void generate_payslip_for_employee(Employee *e) {
    if (!e) return;
    char fname[128];
    time_t t = time(NULL);
    struct tm tm = *localtime(&t);
    snprintf(fname, sizeof(fname), "payslip_%d_%04d%02d%02d.txt", e->id, tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday);
    FILE *f = fopen(fname, "w");
    if (!f) { perror("open payslip"); return; }
    double perf_bonus = (e->performance_score / 100.0) * 500.0;
    double gross = e->base_salary + (e->base_salary * e->bonus_percent / 100.0) + perf_bonus;
    double net = gross - e->deduction;
    fprintf(f, "╔══════════════════════════════════════════════════════════════╗\n");
    fprintf(f, "║                      PAYSLIP STATEMENT                       ║\n");
    fprintf(f, "╚══════════════════════════════════════════════════════════════╝\n\n");
    fprintf(f, "Payslip for %s (ID: %d)\nDate: %04d-%02d-%02d\n\n", e->name, e->id, tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday);
    fprintf(f, "Department: %s\nBase Salary: %.2f\nBonus %%: %.2f\nPerformance Bonus: %.2f\nDeductions: %.2f\n\nGross: %.2f\nNet: %.2f\n", e->department, e->base_salary, e->bonus_percent, perf_bonus, e->deduction, gross, net);
    fclose(f);
    printf("Generated payslip: %s\n", fname);
}

/* ================= Shortlisting students ================= */

void shortlist_students_interactive(void) {
    char filename[256];
    double cpi_cut;
    int min_q;
    int max_time;

    printf("Enter student input filename (default: %s): ", STUDENT_INPUT_DEFAULT);
    while(getchar()!='\n');
    alarm(INPUT_TIMEOUT);
    if (!fgets(filename, sizeof(filename), stdin)) { alarm(0); return; }
    alarm(0);
    filename[strcspn(filename, "\n")] = '\0';
    if (strlen(filename) == 0) strncpy(filename, STUDENT_INPUT_DEFAULT, sizeof(filename));

    printf("CPI cutoff (e.g. 7.0): ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%lf", &cpi_cut) != 1) { alarm(0); while(getchar()!='\n'); printf("Invalid CPI\n"); return; }
    alarm(0);
    
    printf("Minimum questions solved (e.g. 2): ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%d", &min_q) != 1) { alarm(0); while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);
    
    printf("Maximum allowed time in seconds (e.g. 600): ");
    alarm(INPUT_TIMEOUT);
    if (scanf("%d", &max_time) != 1) { alarm(0); while(getchar()!='\n'); printf("Invalid\n"); return; }
    alarm(0);

    if (access(filename, R_OK) != 0) {
        printf("Cannot read file '%s': %s\n", filename, strerror(errno));
        return;
    }

    int p1[2], p2[2];
    if (pipe(p1) == -1) { perror("pipe p1"); return; }
    if (pipe(p2) == -1) { perror("pipe p2"); close(p1[0]); close(p1[1]); return; }

    pid_t pid_grep = fork();
    if (pid_grep < 0) {
        perror("fork grep");
        close(p1[0]); close(p1[1]); close(p2[0]); close(p2[1]);
        return;
    }

    if (pid_grep == 0) {
        dup2(p1[1], STDOUT_FILENO);
        close(p1[0]); close(p1[1]);
        close(p2[0]); close(p2[1]);
        execlp("grep", "grep", "-v", "^$", filename, (char *)NULL);
        perror("execlp grep");
        _exit(127);
    }

    pid_t pid_awk = fork();
    if (pid_awk < 0) {
        perror("fork awk");
        close(p1[0]); close(p1[1]); close(p2[0]); close(p2[1]);
        return;
    }

    if (pid_awk == 0) {
        dup2(p1[0], STDIN_FILENO);
        dup2(p2[1], STDOUT_FILENO);
        close(p1[0]); close(p1[1]); close(p2[0]); close(p2[1]);

        char cpi_arg[64], minq_arg[64], maxt_arg[64];
        snprintf(cpi_arg, sizeof(cpi_arg), "cpi=%g", cpi_cut);
        snprintf(minq_arg, sizeof(minq_arg), "minq=%d", min_q);
        snprintf(maxt_arg, sizeof(maxt_arg), "maxt=%d", max_time);

        const char *awk_prog = "{ if (($3 + 0) >= cpi && ($5 + 0) >= minq && ($4 + 0) <= maxt) print $0 }";

        execlp("awk", "awk", "-v", cpi_arg, "-v", minq_arg, "-v", maxt_arg, awk_prog, (char *)NULL);
        perror("execlp awk");
        _exit(127);
    }

    pid_t pid_sort = fork();
    if (pid_sort < 0) {
        perror("fork sort");
        close(p1[0]); close(p1[1]); close(p2[0]); close(p2[1]);
        return;
    }

    if (pid_sort == 0) {
        int outfd = open(SHORTLISTED_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (outfd == -1) {
            perror("open shortlisted file");
            _exit(127);
        }
        dup2(p2[0], STDIN_FILENO);
        dup2(outfd, STDOUT_FILENO);
        close(outfd);
        close(p1[0]); close(p1[1]); close(p2[0]); close(p2[1]);
        execlp("sort", "sort", "-k3,3nr", "-k5,5nr", "-k4,4n", (char *)NULL);
        perror("execlp sort");
        _exit(127);
    }

    close(p1[0]); close(p1[1]); close(p2[0]); close(p2[1]);

    int status;
    waitpid(pid_grep, &status, 0);
    waitpid(pid_awk, &status, 0);
    waitpid(pid_sort, &status, 0);

    printf("Shortlisting done. Results saved to %s\n", SHORTLISTED_FILE);
}

/* ================= Parent menus ================= */

void payroll_request_and_print(int write_fd, int read_fd) {
    const char *cmd = "RUN_PAYROLL\n";
    if (write(write_fd, cmd, strlen(cmd)) == -1) perror("write payroll");
    char rbuf[512];
    while (1) {
        ssize_t r = read(read_fd, rbuf, sizeof(rbuf)-1);
        if (r <= 0) break;
        rbuf[r] = '\0';
        printf("%s", rbuf);
        if (strstr(rbuf, "PAYROLL_DONE")) break;
    }
}

void manage_performance_menu() {
    int choice;
    do {
        printf("\n--- Performance Menu ---\n");
        printf("1) View All Performance\n2) Update Performance for Employee\n3) Back\nChoice: ");
        alarm(INPUT_TIMEOUT);
        if (scanf("%d", &choice) != 1) { alarm(0); while(getchar()!='\n'); continue; }
        alarm(0);
        
        switch (choice) {
            case 1: view_all_employees(); break;
            case 2: {
                int id; double score;
                printf("Enter Employee ID: ");
                alarm(INPUT_TIMEOUT);
                if (scanf("%d",&id) != 1) { alarm(0); while(getchar()!='\n'); printf("Invalid\n"); break; }
                alarm(0);
                
                load_employees_from_file();
                int idx = find_employee_index_by_id(id);
                if (idx == -1) { printf("Not found\n"); break; }
                
                printf("Enter new performance score (0-100): ");
                alarm(INPUT_TIMEOUT);
                if (scanf("%lf",&score) != 1 || score < 0 || score > 100) { alarm(0); printf("Invalid\n"); break; }
                alarm(0);
                
                emp_list[idx].performance_score = score;
                save_employees_to_file();
                printf("Updated performance.\n");
            } break;
            case 3: return;
            default: printf("Invalid\n");
        }
    } while (choice != 3);
}

void manage_office_hours_menu() {
    int choice;
    do {
        printf("\n--- Office Hours Menu ---\n");
        printf("1) Record Hours for Employee\n2) View Employee Hours\n3) Back\nChoice: ");
        alarm(INPUT_TIMEOUT);
        if (scanf("%d", &choice) != 1) { alarm(0); while(getchar()!='\n'); continue; }
        alarm(0);
        
        switch (choice) {
            case 1: {
                int id; double h;
                printf("Enter Employee ID: ");
                alarm(INPUT_TIMEOUT);
                if (scanf("%d",&id)!=1) { alarm(0); while(getchar()!='\n'); printf("Invalid\n"); break; }
                alarm(0);
                
                int idx = find_employee_index_by_id(id);
                if (idx == -1) { printf("Not found\n"); break; }
                
                printf("Enter hours to add: ");
                alarm(INPUT_TIMEOUT);
                if (scanf("%lf",&h) != 1) { alarm(0); printf("Invalid\n"); break; }
                alarm(0);
                
                emp_list[idx].total_hours += h;
                save_employees_to_file();
                printf("Updated hours: %.2f\n", emp_list[idx].total_hours);
            } break;
            case 2: {
                int id;
                printf("Enter Employee ID: ");
                alarm(INPUT_TIMEOUT);
                if (scanf("%d",&id) != 1) { alarm(0); while(getchar()!='\n'); printf("Invalid\n"); break; }
                alarm(0);
                
                int idx = find_employee_index_by_id(id);
                if (idx == -1) { printf("Not found\n"); break; }
                printf("Employee %s (ID %d) total hours: %.2f\n", emp_list[idx].name, emp_list[idx].id, emp_list[idx].total_hours);
            } break;
            case 3: return;
            default: printf("Invalid\n");
        }
    } while (choice != 3);
}

void recruitment_menu() {
    int choice;
    do {
        printf("\n--- Recruitment Menu ---\n");
        printf("1) Add Applicant\n2) View Applicants\n3) Hire Applicant\n4) Delete Applicant\n5) Back\nChoice: ");
        alarm(INPUT_TIMEOUT);
        if (scanf("%d",&choice) != 1) { alarm(0); while(getchar()!='\n'); continue; }
        alarm(0);
        
        switch (choice) {
            case 1: add_applicant(); break;
            case 2: view_applicants(); break;
            case 3: hire_applicant(); break;
            case 4: delete_applicant(); break;
            case 5: return;
            default: printf("Invalid\n");
        }
    } while (choice != 5);
}

void simple_module_menu(const char *name, const char *file) {
    int c;
    do {
        printf("\n--- %s Menu ---\n", name);
        printf("1) Add Record\n2) View Records\n3) Back\nChoice: ");
        alarm(INPUT_TIMEOUT);
        if (scanf("%d",&c) != 1) { alarm(0); while(getchar()!='\n'); continue; }
        alarm(0);
        
        switch (c) {
            case 1: {
                char buf[512];
                printf("Enter record (single-line): ");
                while(getchar()!='\n');
                alarm(INPUT_TIMEOUT);
                if (!fgets(buf, sizeof(buf), stdin)) { alarm(0); break; }
                alarm(0);
                buf[strcspn(buf,"\n")] = '\0';
                add_record_to_file(file, buf);
                printf("Added.\n");
            } break;
            case 2: view_records(file); break;
            case 3: return;
            default: printf("Invalid\n");
        }
    } while (c != 3);
}

/* ================= Main parent loop ================= */

void parent_main_loop(int pipe_parent_to_child_w, int pipe_child_to_parent_r, pid_t payroll_pid) {
    while (!sig_exit) {
        if (sig_timeout) {
            sig_timeout = 0;
        }
        
        if (sig_backup) {
            sig_backup = 0;
            backup_file(EMP_FILE, BACKUP_FILE);
        }
        
        if (sig_report) {
            sig_report = 0;
            printf("Signal: running payroll report...\n");
            payroll_request_and_print(pipe_parent_to_child_w, pipe_child_to_parent_r);
        }

        if (sigsetjmp(timeout_buf, 1) != 0) {
            /* Timeout occurred at main menu - wait for ENTER, then show menu again */
            alarm(0);
            timeout_enabled = 0;
            getchar(); /* Wait for ENTER key */
            continue;
        }

        clr();
        printf(BOLD BRIGHT_CYAN "\n    ╦ ╦╦═╗╔╦╗╔═╗  ╔═╗╦ ╦╔═╗╔╦╗╔═╗╔╦╗\n" RESET);
        printf(BOLD BRIGHT_CYAN "    ╠═╣╠╦╝║║║╚═╗  ╚═╗╚╦╝╚═╗ ║ ║╣ ║║║\n" RESET);
        printf(BOLD BRIGHT_CYAN "    ╩ ╩╩╚═╩ ╩╚═╝  ╚═╝ ╩ ╚═╝ ╩ ╚═╝╩ ╩\n" RESET);
      
        hdr("HUMAN RESOURCE MANAGEMENT SYSTEM");
        printf("\n" BOLD BRIGHT_WHITE "  EMPLOYEE MANAGEMENT\n" RESET);
        mi(1, "Add Employee"); mi(2, "View All Employees"); mi(3, "Search Employee");
        mi(4, "Update Employee"); mi(5, "Delete Employee");
        printf("\n" BOLD BRIGHT_WHITE "  PAYROLL & COMPENSATION\n" RESET);
        mi(6, "Run Payroll (Multi-threaded)"); mi(7, "Generate Payslip"); mi(8, "Backup Data");
        printf("\n" BOLD BRIGHT_WHITE "  PERFORMANCE & HOURS\n" RESET);
        mi(9, "Manage Performance"); mi(10, "Manage Office Hours");
        printf("\n" BOLD BRIGHT_WHITE "  ADMINISTRATION\n" RESET);
        mi(11, "Policies  Resignation"); mi(12, "Shortlist Students (Custom criteria)");
        printf("\n" BOLD RED "  • 13) Exit System\n" RESET);
        printf("\n"); sep(); pr("Choose:");

        int choice;
        timeout_enabled = 1;
        alarm(INPUT_TIMEOUT);
        if (scanf("%d", &choice) != 1) { 
            alarm(0);
            timeout_enabled = 0;
            while(getchar()!='\n'); 
            continue; 
        }
        alarm(0);
        timeout_enabled = 0;
        
        switch (choice) {
            case 1: add_employee_interactive(); break;
            case 2: view_all_employees(); break;
            case 3: search_employee(); break;
            case 4: update_employee(); break;
            case 5: delete_employee(); break;
            case 6: payroll_request_and_print(pipe_parent_to_child_w, pipe_child_to_parent_r); break;
            case 7: {
                if (sigsetjmp(timeout_buf, 1) != 0) {
                    /* Timeout occurred - wait for ENTER then break to menu */
                    alarm(0);
                    timeout_enabled = 0;
                    getchar(); /* Wait for ENTER key */
                    break;
                }
                
                int id;
                printf("Enter Employee ID for payslip: ");
                timeout_enabled = 1;
                alarm(INPUT_TIMEOUT);
                if (scanf("%d",&id) != 1) { alarm(0); timeout_enabled = 0; while(getchar()!='\n'); printf("Invalid\n"); break; }
                alarm(0);
                timeout_enabled = 0;
                
                load_employees_from_file();
                int idx = find_employee_index_by_id(id);
                if (idx == -1) { printf("Not found\n"); break; }
                generate_payslip_for_employee(&emp_list[idx]);
            } break;
            case 8: backup_file(EMP_FILE, BACKUP_FILE); break;
            case 9: getScholarScore(); break;
            case 10: manage_office_hours_menu(); break;
            case 11: {
                int sub;
                do {
                    printf("\n--- Admin Modules ---\n");
                    printf("1) Policies\n2) Resignation\n");
                    printf("3) Back\nChoice: ");
                    alarm(INPUT_TIMEOUT);
                    if (scanf("%d",&sub) != 1) { alarm(0); while(getchar()!='\n'); continue; }
                    alarm(0);
                    
                    switch (sub) {
                        case 1: simple_module_menu("Policies", POLICIES_FILE); break;
                        
                        case 2: simple_module_menu("Resignation", RESIGNATION_FILE); break;
                  
                        case 3: break;
                        default: printf("Invalid\n");
                    }
                } while (sub != 8);
            } break;
            case 12: shortlist_students_interactive(); break;
            case 13: sig_exit = 1; break;
            default: printf("Invalid choice.\n");
        }
    }

    const char *cmd = "EXIT\n";
    if (write(pipe_parent_to_child_w, cmd, strlen(cmd)) == -1) perror("write EXIT to payroll");
    int status;
    waitpid(payroll_pid, &status, 0);
    printf("Parent: payroll child terminated.\n");
}

/* ================= Main entrypoint ================= */

int main(int argc, char *argv[]) {
    struct sigaction sa;
    sa.sa_handler = parent_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGINT, &sa, NULL) == -1) fatal("sigaction SIGINT");
    if (sigaction(SIGUSR1, &sa, NULL) == -1) fatal("sigaction SIGUSR1");
    if (sigaction(SIGUSR2, &sa, NULL) == -1) fatal("sigaction SIGUSR2");
    if (sigaction(SIGALRM, &sa, NULL) == -1) fatal("sigaction SIGALRM");

    clr();
    printf(BOLD BRIGHT_CYAN "\n\n");
    printf("    ╔══════════════════════════════════════════════════════════════════════╗\n");
    printf("    ║                                                                      ║\n");
    printf("    ║              WELCOME TO HR MANAGEMENT SYSTEM                        ║\n");
    printf("    ║                                                                      ║\n");
    printf("    ║              Powered by Multi-threading & IPC                       ║\n");
    printf("    ║                                                                      ║\n");
    printf("    ╚══════════════════════════════════════════════════════════════════════╝\n" RESET);

    load_employees_from_file();

    int ptoc[2], ctop[2];
    if (pipe(ptoc) == -1) fatal("pipe ptoc");
    if (pipe(ctop) == -1) fatal("pipe ctop");

    pid_t pid = fork();
    if (pid < 0) fatal("fork");
    if (pid == 0) {
        close(ptoc[1]);
        close(ctop[0]);
        payroll_process_loop(ptoc[0], ctop[1]);
        _exit(0);
    } else {
        close(ptoc[0]);
        close(ctop[1]);

        parent_main_loop(ptoc[1], ctop[0], pid);

        close(ptoc[1]); close(ctop[0]);
        printf("HRMS shutting down cleanly.\n");
    }

    return 0;
}