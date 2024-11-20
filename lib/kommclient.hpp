#ifndef CLIENT_H_
#include <string>
#define CLIENT_H_

int komm_open(std::string path);
int komm_stat(std::string pathname, struct stat*statbuf);
ssize_t komm_pwrite(int fd, const void* buf, size_t count, off_t offset);
ssize_t komm_pread(int fd, void* buf, size_t count, off_t offset);
int komm_unlink(std::string path);
int komm_close(int fd);


#endif
