#include <stdio.h>
#include <unistd.h>

int main(int argc, char **argv) {
    (void)argc;
    const char *path = "/usr/local/lib/session-runtime/portable";
#if defined(__x86_64__)
    if (__builtin_cpu_supports("x86-64-v3"))
        path = "/usr/local/lib/session-runtime/v3";
#endif
    execv(path, argv);
    perror(path);
    return 127;
}
