//            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
//                    Version 2, December 2004
//
// Copyleft (ↄ) meh. <meh@schizofreni.co> | http://meh.schizofreni.co
//
// Everyone is permitted to copy and distribute verbatim or modified
// copies of this license document, and changing it is allowed as long
// as the name is changed.
//
//            DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
//   TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION
//
//  0. You just DO WHAT THE FUCK YOU WANT TO.

//! Bindings to internal Linux stuff.

use libc::{c_int, ifreq, ioctl, Ioctl};

// 定义 SIOCGIF* 和 SIOCSIF* 命令号
const SIOCGIFFLAGS: Ioctl = 0x8913;
const SIOCSIFFLAGS: Ioctl = 0x8914;
const SIOCGIFADDR: Ioctl = 0x8915;
const SIOCSIFADDR: Ioctl = 0x8916;
const SIOCGIFDSTADDR: Ioctl = 0x8917;
const SIOCSIFDSTADDR: Ioctl = 0x8918;
const SIOCGIFBRDADDR: Ioctl = 0x8919;
const SIOCSIFBRDADDR: Ioctl = 0x891a;
const SIOCGIFNETMASK: Ioctl = 0x891b;
const SIOCSIFNETMASK: Ioctl = 0x891c;
const SIOCGIFMTU: Ioctl = 0x8921;
const SIOCSIFMTU: Ioctl = 0x8922;
const SIOCSIFNAME: Ioctl = 0x8923;

// 定义 TUNSET* 命令号
const TUNSETIFF: Ioctl = ((b'T' as Ioctl) << 8) | 202;
const TUNSETPERSIST: Ioctl = ((b'T' as Ioctl) << 8) | 203;
const TUNSETOWNER: Ioctl = ((b'T' as Ioctl) << 8) | 204;
const TUNSETGROUP: Ioctl = ((b'T' as Ioctl) << 8) | 206;

// SIOCGIF* 读取函数
unsafe fn siocgifflags(fd: c_int, req: *mut ifreq) -> c_int {
    ioctl(fd, SIOCGIFFLAGS, req)
}

unsafe fn siocgifaddr(fd: c_int, req: *mut ifreq) -> c_int {
    ioctl(fd, SIOCGIFADDR, req)
}

unsafe fn siocgifdstaddr(fd: c_int, req: *mut ifreq) -> c_int {
    ioctl(fd, SIOCGIFDSTADDR, req)
}

unsafe fn siocgifbrdaddr(fd: c_int, req: *mut ifreq) -> c_int {
    ioctl(fd, SIOCGIFBRDADDR, req)
}

unsafe fn siocgifnetmask(fd: c_int, req: *mut ifreq) -> c_int {
    ioctl(fd, SIOCGIFNETMASK, req)
}

unsafe fn siocgifmtu(fd: c_int, req: *mut ifreq) -> c_int {
    ioctl(fd, SIOCGIFMTU, req)
}

// SIOCSIF* 写入函数
unsafe fn siocsifflags(fd: c_int, req: *const ifreq) -> c_int {
    ioctl(fd, SIOCSIFFLAGS, req)
}

unsafe fn siocsifaddr(fd: c_int, req: *const ifreq) -> c_int {
    ioctl(fd, SIOCSIFADDR, req)
}

unsafe fn siocsifdstaddr(fd: c_int, req: *const ifreq) -> c_int {
    ioctl(fd, SIOCSIFDSTADDR, req)
}

unsafe fn siocsifbrdaddr(fd: c_int, req: *const ifreq) -> c_int {
    ioctl(fd, SIOCSIFBRDADDR, req)
}

unsafe fn siocsifnetmask(fd: c_int, req: *const ifreq) -> c_int {
    ioctl(fd, SIOCSIFNETMASK, req)
}

unsafe fn siocsifmtu(fd: c_int, req: *const ifreq) -> c_int {
    ioctl(fd, SIOCSIFMTU, req)
}

unsafe fn siocsifname(fd: c_int, req: *const ifreq) -> c_int {
    ioctl(fd, SIOCSIFNAME, req)
}

// TUNSET* 写入函数
unsafe fn tunsetiff(fd: c_int, req: *const c_int) -> c_int {
    ioctl(fd, TUNSETIFF, req)
}

unsafe fn tunsetpersist(fd: c_int, req: *const c_int) -> c_int {
    ioctl(fd, TUNSETPERSIST, req)
}

unsafe fn tunsetowner(fd: c_int, req: *const c_int) -> c_int {
    ioctl(fd, TUNSETOWNER, req)
}

unsafe fn tunsetgroup(fd: c_int, req: *const c_int) -> c_int {
    ioctl(fd, TUNSETGROUP, req)
}
