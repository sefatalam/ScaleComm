#include <iostream>
#include <string>
#include <thallium.hpp>
#include <unordered_map>
#include <thallium/serialization/stl/string.hpp>
#include <fstream>      // std::fstream
#include <filesystem>
#include <cereal/types/memory.hpp>
#include <sys/types.h>
#include <unistd.h>
#include <sys/stat.h>
#include <bits/stdc++.h> 
#include <errno.h>
#include <sstream>
#include <fcntl.h>
namespace tl = thallium;

class statistics {

    private:

        int size;
        std::string last_access;
        std::string last_modified;

    public:
        
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

class Engine
{
private:
   tl::engine engine;
   Engine(){};

public:
   static Engine& instance()
   {
      static Engine instance;
      return instance;
   }
   void setAddress(std::string arg1, int arg2){
      tl::engine tmp_engine("ofi+tcp://"+ arg1 +":41819", THALLIUM_SERVER_MODE, 0, arg2);
      this->engine = tmp_engine;
      }
   tl::engine& getEngine(){
    return engine;
   }

};

Engine& me = Engine::instance();
    
tl::engine& myEngine = me.getEngine();
std::unordered_map<unsigned int,std::string> fileDescriptors;
int counter = 0;

template<typename A>
void serialize(A& ar, struct stat& x){
	ar(x);
}



void komm_pwrite(const tl::request& req, tl::bulk b, unsigned int fd, size_t count, off_t offset){
  //std::fstream fs;
  int fi = open(fileDescriptors[fd].c_str(), O_WRONLY | O_CREAT);
  
	//fs.open(fileDescriptors[fd], std::fstream::out);
  tl::endpoint ep = req.get_endpoint();
  std::vector<char> v((long)count);
  std::vector<std::pair<void*,std::size_t>> segments(1);
  segments[0].first  = (void*)(&v[0]);
  segments[0].second = count;
  tl::bulk local = myEngine.expose(segments, tl::bulk_mode::write_only);
  b.on(ep) >> local;
  // fs.seekp(offset);
	// fs << std::string(v.begin(), v.end());
  // fs.close();
  int written = pwrite(fi, segments[0].first, count, offset);
  req.respond(written);
	return; 
	
	
}
void komm_pread(const tl::request& req,tl::bulk recBulk, unsigned int fd, size_t count, off_t offset){
  //std::fstream fs;
  tl::endpoint ep = req.get_endpoint();
	//fs.open(fileDescriptors[fd], std::fstream::in);
  int fi = open(fileDescriptors[fd].c_str(), O_RDONLY);
  char * buffer = new char [count];
  // fs.seekg(offset);
  // fs.read(buffer, count);
  int read = pread(fi, buffer, count, offset);
  std::vector<std::pair<void*, std::size_t>> segments(1);
  segments[0].first = (void*)(buffer);
  segments[0].second = count;
  tl::bulk sendBulk = myEngine.expose(segments, tl::bulk_mode::read_only);
  sendBulk >> recBulk.on(ep);
  req.respond(read);
	return; 
	
	
}



void komm_close(const tl::request& req, int fd) {
	fileDescriptors.erase(fd);
	req.respond(0);
	return;
}


void komm_open(const tl::request& req,  std::string path){
    // for (auto &i : fileDescriptors) {
    //   if (i.second.first == path) {
    //      req.respond(-1);
    //      return; // to stop searching
    //   }
    // }
    //struct stat buffer;
    //if(!(stat (pathname, &buffer) == 0)){
    int fi = open(path.c_str(),O_RDWR | O_CREAT);
    //}
    fileDescriptors[counter] = path;

    req.respond(counter++);
    return;
}



void file_stat(const tl::request& req,  std::string pathname){
  struct stat buf_c;
  stat(pathname.c_str(), &buf_c);
  struct statistics buffer(buf_c);
  req.respond(buffer);
  return;
}


void komm_unlink(const tl::request& req, std::string pathname){
  req.respond(std::remove(pathname.c_str()));
}



int main(int argc, char** argv) {
    if (argc != 3){
      std::cerr << "Usage: " << argv[0] << " <address>" << "<threads>" <<std::endl;
      exit(0);
    }
    std::string arg1(argv[1]);
    int arg2 = std::stoi(argv[2]);
    me.setAddress(arg1,arg2);

    std::cout << "Server running at address " << myEngine.self() << std::endl;

    myEngine.define("open", komm_open);
    myEngine.define("stat", file_stat);
    myEngine.define("unlink", komm_unlink);
    myEngine.define("pwrite", komm_pwrite);
    myEngine.define("pread", komm_pread);
    myEngine.define("close",komm_close);

    return 0;
}


