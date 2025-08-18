#include<iostream>
#include<fstream>
#include<queue>
#include<vector>
#include<string>
#include<cstdio>
#include<cstdlib>
#include<ctime>
#include<iomanip>
#include<sstream>
#include<unistd.h>
#include<netdb.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<sys/epoll.h>
#include<functional>
#include<thread>
#include<mutex>
#include<chrono>
#include<condition_variable>
using namespace std;

//获取系统时间的工具函数
string getCurrenttiem(){
    auto now=std::chrono::system_clock::now();
    time_t current=std::chrono::system_clock::to_time_t(now);
    std::tm localTime;
    localtime_r(&current,&localTime);
    ostringstream oss;
    oss<<std::put_time(&localTime,"%Y-%m-%d %H:%M:%S");
    return oss.str()+"\n";
}

//依据生产者消费模型对tcpServer的响应作多线程处理
class LinkPool{
private:
    static LinkPool*pool;
    static once_flag flag;
    static int epfd;    
private:
    queue<int>recvPool;
    vector<thread> recv_workers;
    mutex recv_mutex;
    condition_variable cv;
    bool stop;
    int epoll_fd;
private:
    LinkPool(int num,int ep):stop(false),epoll_fd(ep){
        for(int i=0;i<num;i++){
            recv_workers.emplace_back([this](){
                int cfd=-1;
                string file_name;
                while(true){ 
                    {
                        unique_lock<mutex> lock(recv_mutex);
                        cv.wait(lock,[this](){return !recvPool.empty()||stop;});
                        if(stop&&recvPool.empty()){
                            return;
                        }
                        cfd=recvPool.front();
                        recvPool.pop();
                    }
                    char buffer[1024];
                    int readn=-1;
                    file_name.clear();
                    file_name="../exe/"+to_string(cfd)+".txt";
                    while((readn=recv(cfd,buffer,1024,0))>=1){
                        buffer[readn]='\0';
                        string str(buffer);
                        ofstream fd_write(file_name,ios::app);
                        fd_write<<str;
                        fd_write.close();
                        if(readn<1024) break;//说明此时已经读完缓冲区

                    }
                    if(readn==0){
                        epoll_ctl(epoll_fd,EPOLL_CTL_DEL,cfd,NULL);
                        close(cfd);
                    }                    

                }
            });
        }
    };
    LinkPool(const LinkPool&)=delete;
    LinkPool& operator=(const LinkPool&)=delete;
    ~LinkPool(){
        {
            unique_lock<mutex> lock(recv_mutex);
            stop=true;
        }
        cv.notify_all();
        for(auto& it:recv_workers){
            if(it.joinable()){
                it.join();
            }
        }
    };
public:
    void addFd(int clientfd){
        {
            unique_lock<mutex> lock(recv_mutex);
            recvPool.push(clientfd);
        }
        cv.notify_one();
    }
public:
   static LinkPool* GetLinkPool(int num=10){
    call_once(flag,[num](){pool=new LinkPool(num,epfd);});
    return pool;
   } 
   static void closePool(){
    if(pool){
        delete pool;
        pool=nullptr;
    }
   }

};

LinkPool* LinkPool::pool=nullptr;
once_flag LinkPool::flag;
int LinkPool::epfd=-1;


//此类采用单例模式设计
class tcpServer{
private:
    static tcpServer*server;
    static once_flag flag;
    static bool stop;
private:
    tcpServer(){}
    tcpServer(const tcpServer&)=delete;
    tcpServer& operator=(const tcpServer&)=delete;
    ~tcpServer(){
        log_record<<"Exited the tcpServer"<<getCurrenttiem()<<endl;        
    };
private:
    static int lfd;
    static int epfd;
    static ofstream log_record;
    static LinkPool*pool;

public:
    //该函数的目标是绑定服务端的信息以及创建epoll即可
    bool initService(unsigned short port=5050){
        log_record=ofstream("../log_record.txt",ios::app);
        if(!log_record.is_open()){
            cerr<<"open log file failed!"<<endl;
            return false;
        }
        if((lfd=socket(AF_INET,SOCK_STREAM,0))<0){
            log_record<<"Open Listen fd failed! "<<getCurrenttiem()<<endl;
            return false;
        }
        if((epfd=epoll_create(100))<0){
            log_record<<"Open Epoll fd failed! "<<getCurrenttiem()<<endl;
            close(lfd);
            return false;
        }
        struct sockaddr_in servaddr;
        servaddr.sin_family=AF_INET;
        servaddr.sin_addr.s_addr=htonl(INADDR_ANY);
        servaddr.sin_port=htons(port);
        if(bind(lfd,(struct sockaddr*)&servaddr,sizeof(servaddr))!=0){
            log_record<<"Bind error"<<getCurrenttiem()<<endl;
            return false;
        }
        if(listen(lfd,5)!=0){
            log_record<<"Listen Error! "<<getCurrenttiem()<<endl;
            return false;
        }
        return true;
    }
    bool launchServer(){
        int clientfd=-1;
        struct epoll_event ev;
        ev.events=EPOLLIN;
        ev.data.fd=lfd;
        int ret=epoll_ctl(epfd,EPOLL_CTL_ADD,lfd,&ev);
        struct epoll_event epoll_events[100];
        while(!this->stop){
            int num=0;
            if((num=epoll_wait(epfd,epoll_events,100,-1))>=0){
                for(int i=0;i<num;i++){
                    if(epoll_events[i].data.fd==lfd){
                        int cfd=accept(lfd,NULL,NULL);
                        struct epoll_event ev;
                        ev.data.fd=cfd;
                        ev.events=EPOLLIN;
                        epoll_ctl(epfd,EPOLL_CTL_ADD,cfd,&ev);
                    }else if(epoll_events[i].data.fd!=lfd&&epoll_events[i].events==EPOLLIN){
                        pool->addFd(epoll_events[i].data.fd);
                    }
                }
            }else{
                log_record<<"Epfd Error"<<getCurrenttiem()<<endl;    
                return false;
            }
        }
        return true;
    }
public:
    static tcpServer* GetServer(){
        call_once(flag,[](){server=new tcpServer();pool=LinkPool::GetLinkPool();});
        return server;
    }
    static void CloseServer(){
        stop=true;
        pool->closePool();
        if(server){
            delete server;
            server=nullptr;
        }
        if(log_record.is_open()){
            log_record.close();
        }
        if(lfd!=-1) close(lfd);
        if(epfd!=-1) close(epfd);
        
    }
};

tcpServer* tcpServer::server=nullptr;
once_flag tcpServer::flag;
int tcpServer::lfd=-1;
int tcpServer::epfd=-1;
ofstream tcpServer::log_record;
bool tcpServer::stop=false;
LinkPool* tcpServer::pool=nullptr;

//启动测试
int main(int argc,char* argv[]){
    tcpServer* server=tcpServer::GetServer();
    if(server->initService()){
        server->launchServer();
        cout<<"finished!"<<endl;
    }else{
        cerr<<"initService error"<<endl;
        server->CloseServer();
    }
  
}