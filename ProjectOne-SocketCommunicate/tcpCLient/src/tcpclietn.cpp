#include <iostream>
#include <cstring>
#include <string>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
using namespace std;

class client {
private:
    int fd;

public:
    client() : fd(-1) {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            cerr << "socket creation failed!" << endl;
            exit(1);
        }
    }

    ~client() {
        if (fd != -1) {
            close(fd);
        }
    }

    void initclient(unsigned short port, const string& ip) {
        struct sockaddr_in servaddr;
        memset(&servaddr, 0, sizeof(servaddr));
        servaddr.sin_family = AF_INET;
        servaddr.sin_port = htons(port);

        struct hostent* h = gethostbyname(ip.c_str());
        if (h == nullptr) {
            cerr << "gethostbyname failed for: " << ip << endl;
            return;
        }

        servaddr.sin_addr = *(struct in_addr*)h->h_addr_list[0];

        if (connect(fd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
            cerr << "connect failed!" << endl;
            return;
        }

        for (int i = 0; i < 2; i++) {
            string str = "hi, i am cjf!\n";
            int n = send(fd, str.c_str(), str.size(), 0);
            if (n < 0) {
                cerr << "send failed!" << endl;
                return;
            }
            cout << "Sent: " << str;
        }

        // 可选：关闭写端，通知服务端
        // shutdown(fd, SHUT_WR);
    }
};

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " <ip> <port>" << endl;
        return 1;
    }

    client cl;
    cl.initclient(atoi(argv[2]), argv[1]);

    return 0;
}