#include <iostream>
#include <string>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <vector>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include<tuple> // for tuple
#include <string>
#include <sstream>
#include <ctime>

namespace tl = thallium;
std::vector<std::string> Server_Ips{"192.168.1.1", "192.168.1.1", "192.168.1.1"};
std::unordered_map<int, std::pair<int,int>> id2ip;
tl::engine myEngine("tcp", THALLIUM_CLIENT_MODE);
tl::remote_procedure open_r = myEngine.define("open");
tl::remote_procedure stat_r = myEngine.define("stat");
tl::remote_procedure komm_pwrite_r = myEngine.define("pwrite");
tl::remote_procedure komm_pread_r = myEngine.define("pread");
tl::remote_procedure unlink_r = myEngine.define("unlink");
tl::remote_procedure close_r = myEngine.define("close");
std::vector<tl::endpoint> endpoints{myEngine.lookup("ofi+tcp://"+Server_Ips[0]+":41819"),myEngine.lookup("ofi+tcp://"+Server_Ips[1]+":41819"),myEngine.lookup("ofi+tcp://"+Server_Ips[2]+":41819")};

unsigned int counterID = 0;

class statistics {

    private:

    public:

        int size;
        std::string last_access;
        std::string last_modified;
        
        statistics(struct stat buf={}){
            size = int(buf.st_size);
            last_access = std::ctime(&buf.st_atime);
            last_modified = std::ctime(&buf.st_mtime);
        }

        template<typename A>
        void serialize(A& ar) {
             ar & size;
             ar & last_access;
             ar & last_modified;
        }
};

template<typename A>
void serialize(A& ar, struct stat& x){
	ar(x);
}

size_t hashResource(const std::string &resource, int range){
    
    std::hash<std::string> hasher;
    return hasher(resource) % range;
}

int komm_open(std::string path){
    size_t hashValue = hashResource(path,Server_Ips.size());
    tl::endpoint server = endpoints[hashValue];
    int id = open_r.on(server)(path);
    id2ip[counterID] = std::make_pair(id, hashValue);
    return counterID++;
}

time_t stringToTime(std::string timeString) {
    std::tm tm = {};
    std::istringstream ss(timeString);
    std::string temp;

    std::unordered_map<std::string,int> dayOfWeekMap;
    dayOfWeekMap["Mon"] = 0;
    dayOfWeekMap["Tue"] = 1;
    dayOfWeekMap["Wed"] = 2;
    dayOfWeekMap["Thu"] = 3;
    dayOfWeekMap["Fri"] = 4;
    dayOfWeekMap["Sat"] = 5;
    dayOfWeekMap["Sun"] = 6;

    std::unordered_map<std::string,int> monthMap;
    monthMap["Jan"] = 0;
    monthMap["Feb"] = 1;
    monthMap["Mar"] = 2;
    monthMap["Apr"] = 3;
    monthMap["May"] = 4;
    monthMap["Jun"] = 5;
    monthMap["Jul"] = 6;
    monthMap["Aug"] = 7;
    monthMap["Sep"] = 8;
    monthMap["Oct"] = 9;
    monthMap["Nov"] = 10;
    monthMap["Dec"] = 11;

    tm.tm_wday = dayOfWeekMap[timeString.substr(0, 3)];
    tm.tm_mon = monthMap[timeString.substr(4, 3)];
    tm.tm_mday = stoi(timeString.substr(8, 2));
    tm.tm_hour = stoi(timeString.substr(11, 2));
    tm.tm_min = stoi(timeString.substr(14, 2));
    tm.tm_sec = stoi(timeString.substr(17, 2));
    tm.tm_year = stoi(timeString.substr(20, 4));
    tm.tm_year -= 1900;
    tm.tm_isdst = -1; // DST information not available


    time_t result = std::mktime(&tm);
    if (result == -1) {
        throw std::runtime_error("Fehler beim Konvertieren von tm zu time_t");
    }
    return result;
}

int komm_stat(std::string pathname, struct stat*statbuf){
    size_t hashValue = hashResource(pathname,Server_Ips.size());
    //std::string arg1(pathname);
    tl::endpoint server = endpoints[hashValue];
    
    struct statistics tpl = stat_r.on(server)(pathname);

    time_t result = tpl.last_access.size()!=0 ? stringToTime(tpl.last_access): 0;

    (*statbuf).st_size = off_t(tpl.size);
    //std::time_t temp_time = std::time(snullptr);
    //std::string timestr = std::ctime(&temp_time);
    //(*statbuf).st_atime = time_t(tpl.last_access); 
    statbuf->st_mtime = result; 

    return 0;


}

ssize_t komm_pwrite(int fd, const void* buf, size_t count, off_t offset){
    int remoteID = id2ip[fd].first;
    tl::endpoint server = endpoints[id2ip[fd].second];
    std::vector<std::pair<void*, std::size_t>> segments(1);
    segments[0].first = (void*)(buf);
    segments[0].second = count;
    tl::bulk sendBulk = myEngine.expose(segments, tl::bulk_mode::read_only);
    ssize_t ret = komm_pwrite_r.on(server)(sendBulk, remoteID, count, offset);
    
    return ret;
}

ssize_t komm_pread(int fd, void* buf, size_t count, off_t offset) {
    int remoteID = id2ip[fd].first;
    tl::endpoint server = endpoints[id2ip[fd].second];
    std::vector<std::pair<void*, std::size_t>> segments(1);
    segments[0].first = buf;
    segments[0].second = count;
    tl::bulk recBulk = myEngine.expose(segments, tl::bulk_mode::write_only);
    ssize_t ret = komm_pread_r.on(server)(recBulk, remoteID, count, offset);

    return ret;
}

int komm_unlink(std::string path){
    size_t hashValue = hashResource(path,Server_Ips.size());
    tl::endpoint server = endpoints[hashValue];
    //std::string pathname(path);
    int ret = unlink_r.on(server)(path);
    return ret;
}

int komm_close(int fd){
    int remoteID = id2ip[fd].first;
    tl::endpoint server = endpoints[id2ip[fd].second];
    int ret = close_r.on(server)(remoteID);
    return ret;
}

