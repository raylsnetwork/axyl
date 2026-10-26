// LD_PRELOAD shim for the one-way link-drop variant of the live reproduction. Test use only.
//
// It plays an on-path attacker that cuts one direction of a node's links: while the file named by
// RL_UDP_DROP_FILE exists, every UDP datagram the process receives is discarded (the caller sees
// EAGAIN, as if nothing had arrived). What the process sends still goes out, so its peers keep
// hearing from it.
//
// An attacker on the path sees when the node starts a new QUIC handshake. To stop the drop at that
// moment, create the file named by RL_UDP_LIFT_FILE: the next QUIC Initial packet the process sends
// removes both files, so the drop ends as the node redials.
//
// The node binary is not changed: the shim only interposes recvmsg, recvmmsg, recvfrom, sendmsg,
// sendmmsg and sendto from libc. Build: cc -shared -fPIC -O2 -o udp_drop.so udp_drop.c -ldl

#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>

static int is_udp(int fd) {
    int type = 0;
    socklen_t len = sizeof(type);
    return getsockopt(fd, SOL_SOCKET, SO_TYPE, &type, &len) == 0 && type == SOCK_DGRAM;
}

static int dropping(int fd) {
    const char *path = getenv("RL_UDP_DROP_FILE");
    return path && access(path, F_OK) == 0 && is_udp(fd);
}

// A QUIC v1 Initial packet: long header, fixed bit, packet type 0.
static void lift_on_initial(int fd, const void *buf, size_t len) {
    const char *lift = getenv("RL_UDP_LIFT_FILE");
    const char *drop = getenv("RL_UDP_DROP_FILE");
    if (!lift || !drop || len == 0 || access(lift, F_OK) != 0 || !is_udp(fd)) return;
    if ((((const unsigned char *)buf)[0] & 0xF0) != 0xC0) return;
    unlink(drop);
    unlink(lift);
}

ssize_t recvmsg(int fd, struct msghdr *msg, int flags) {
    static ssize_t (*real)(int, struct msghdr *, int);
    if (!real) real = dlsym(RTLD_NEXT, "recvmsg");
    ssize_t n = real(fd, msg, flags);
    if (n >= 0 && dropping(fd)) { errno = EAGAIN; return -1; }
    return n;
}

int recvmmsg(int fd, struct mmsghdr *vec, unsigned int vlen, int flags, struct timespec *timeout) {
    static int (*real)(int, struct mmsghdr *, unsigned int, int, struct timespec *);
    if (!real) real = dlsym(RTLD_NEXT, "recvmmsg");
    int n = real(fd, vec, vlen, flags, timeout);
    if (n > 0 && dropping(fd)) { errno = EAGAIN; return -1; }
    return n;
}

ssize_t recvfrom(int fd, void *buf, size_t len, int flags, struct sockaddr *addr, socklen_t *alen) {
    static ssize_t (*real)(int, void *, size_t, int, struct sockaddr *, socklen_t *);
    if (!real) real = dlsym(RTLD_NEXT, "recvfrom");
    ssize_t n = real(fd, buf, len, flags, addr, alen);
    if (n >= 0 && dropping(fd)) { errno = EAGAIN; return -1; }
    return n;
}

ssize_t sendmsg(int fd, const struct msghdr *msg, int flags) {
    static ssize_t (*real)(int, const struct msghdr *, int);
    if (!real) real = dlsym(RTLD_NEXT, "sendmsg");
    if (msg && msg->msg_iovlen > 0) lift_on_initial(fd, msg->msg_iov[0].iov_base, msg->msg_iov[0].iov_len);
    return real(fd, msg, flags);
}

int sendmmsg(int fd, struct mmsghdr *vec, unsigned int vlen, int flags) {
    static int (*real)(int, struct mmsghdr *, unsigned int, int);
    if (!real) real = dlsym(RTLD_NEXT, "sendmmsg");
    for (unsigned int i = 0; i < vlen; i++) {
        if (vec[i].msg_hdr.msg_iovlen > 0) {
            lift_on_initial(fd, vec[i].msg_hdr.msg_iov[0].iov_base, vec[i].msg_hdr.msg_iov[0].iov_len);
        }
    }
    return real(fd, vec, vlen, flags);
}

ssize_t sendto(int fd, const void *buf, size_t len, int flags, const struct sockaddr *addr, socklen_t alen) {
    static ssize_t (*real)(int, const void *, size_t, int, const struct sockaddr *, socklen_t);
    if (!real) real = dlsym(RTLD_NEXT, "sendto");
    lift_on_initial(fd, buf, len);
    return real(fd, buf, len, flags, addr, alen);
}
