#include <iostream>
#include <mpi.h>
#include <lib/kommclient.hpp>
#include <sys/time.h>
#include <sys/stat.h>
#include <vector>
#include <cstring>
#include <fstream>
#include <chrono>
#include <cmath>

#define BENCHMARK_RUNS 10000

unsigned long long PACKAGE_SIZE = 5000000000;
int myrank, size;

std::vector<long> benchmarkOpenStatDelete(std::string root, std::string filename = "tmpFile"){
	const char* path = (root+filename+std::to_string(myrank)).c_str();
	long ops = 0;
	struct stat statbuf;
	
	std::chrono::_V2::system_clock::rep totalTimeOpen=0;
	for(int i=0; i < BENCHMARK_RUNS; ++i){
		std::string fullpath = (path + std::to_string(i));
		auto startOpen = std::chrono::high_resolution_clock::now();
		komm_open(fullpath);
		auto endOpen = std::chrono::high_resolution_clock::now();
		totalTimeOpen += std::chrono::duration_cast<std::chrono::nanoseconds>(endOpen-startOpen).count();
		ops++;
	}
	long throughputOpen = (ops*1000000000 / (totalTimeOpen));
	ops=0;
	MPI_Barrier(MPI_COMM_WORLD);

	std::chrono::_V2::system_clock::rep totalTimeStat=0;
	for(int i=0; i < BENCHMARK_RUNS; ++i){
		std::string fullpath = (path + std::to_string(i));
		auto startStat = std::chrono::high_resolution_clock::now();
		komm_stat(fullpath, &statbuf);
		auto endStat = std::chrono::high_resolution_clock::now();
		ops++;
		totalTimeStat += std::chrono::duration_cast<std::chrono::nanoseconds>(endStat-startStat).count();
	}
	long throughputStat = (ops*1000000000 / totalTimeStat);
	ops= 0;
	MPI_Barrier(MPI_COMM_WORLD);

	std::chrono::_V2::system_clock::rep totalTimeUnlink=0;
	for(int i=0; i < BENCHMARK_RUNS; ++i){
		std::string fullpath = (path + std::to_string(i));
		auto startUnlink = std::chrono::high_resolution_clock::now();
		komm_unlink(fullpath);
		auto endUnlink = std::chrono::high_resolution_clock::now();
		ops++;
		totalTimeUnlink += std::chrono::duration_cast<std::chrono::nanoseconds>(endUnlink-startUnlink).count();
	}
	long throughputUnlink= (ops*1000000000 / totalTimeUnlink);
	ops= 0;
	MPI_Barrier(MPI_COMM_WORLD);

	std::vector<long> throughput = {throughputOpen, throughputStat, throughputUnlink};
	return throughput;
}

std::vector<unsigned long> benchmarkWriteReadFile(std::string root, int fractions){
	
	unsigned int packageSize = std::ceil(PACKAGE_SIZE / fractions);
	
	std::string writeString(packageSize, 'a');
	const void* sendBuffer = (void*)(writeString.c_str());
	
	std::vector<char> receiveString(packageSize);
	void* receiveBuffer = (void*)(&receiveString[0]);
	std:: string openPath = root+"RWFileFractured"+std::to_string(myrank);
	int fd = komm_open(openPath);
	MPI_Barrier(MPI_COMM_WORLD);

	std::chrono::_V2::system_clock::rep totalTimeWrite = 0;	
	for(int i = 0; i < fractions; ++i){
		auto startWrite = std::chrono::high_resolution_clock::now();
		komm_pwrite(fd, sendBuffer, packageSize, packageSize * i);
		auto endWrite = std::chrono::high_resolution_clock::now();
		totalTimeWrite += std::chrono::duration_cast<std::chrono::microseconds>(endWrite-startWrite).count();
	}
	unsigned long long throughputWrite = (PACKAGE_SIZE*1000000) / totalTimeWrite;
	MPI_Barrier(MPI_COMM_WORLD);

	//system("sync; echo 3 > /proc/sys/vm/drop_caches" );

	std::chrono::_V2::system_clock::rep totalTimeRead=0;	
	for(int i = 0; i < fractions; ++i){
		auto startRead = std::chrono::high_resolution_clock::now();
		komm_pread(fd, receiveBuffer, packageSize, packageSize * i);
		auto endRead = std::chrono::high_resolution_clock::now();
		totalTimeRead += std::chrono::duration_cast<std::chrono::microseconds>(endRead-startRead).count();
	}
	unsigned long throughputRead = (PACKAGE_SIZE*1000000) / totalTimeRead;
	std::vector<unsigned long> resTotal = {PACKAGE_SIZE, packageSize, throughputWrite, throughputRead};

	return resTotal;
}





int main(int argc, char *argv[]){
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &myrank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	
	//std::vector<long> init_buffer(size, 0);
	int init_buffer;
	std::string path = "/home/student/BenchMark2/";
	std::fstream fs;
	fs.open(path+"log.txt", std::fstream::out);
	fs.close();
	std::vector<long> osd = benchmarkOpenStatDelete(path);
	//std::vector<long> res_buffer(size);
	int res_buffer;

	//pull open data
	init_buffer = osd[0];
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Reduce(&init_buffer, &res_buffer, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
	
	if(myrank == 0){
		std::cout << "Open " + std::to_string(BENCHMARK_RUNS) + " files: " + std::to_string(res_buffer/size) + " OP/s average" << std::endl;
	}

	//pull stat data
	init_buffer= osd[1];
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Reduce(&init_buffer, &res_buffer, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
	
	if(myrank == 0){
		std::cout << "Stat " + std::to_string(BENCHMARK_RUNS) + " files: " + std::to_string(res_buffer/size) + " OP/s average" << std::endl;
	}

	//pull unlink data
	init_buffer= osd[2];
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Reduce(&init_buffer, &res_buffer, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
	if(myrank == 0){
		std::cout << "Unlink " + std::to_string(BENCHMARK_RUNS) + " files: " + std::to_string(res_buffer/size) + " OP/s average" << std::endl;
	}

	unsigned long init_buffer_long = 0;
	unsigned long res_buffer_long = 0;
	std::vector<int> sizes{5,50,500,5000, 50000};
	for(int i=0; i < sizes.size(); i ++){
		std::vector<unsigned long> readWriteFrac = benchmarkWriteReadFile(path, sizes[i]);
		init_buffer_long = readWriteFrac[2];
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Reduce(&init_buffer_long, &res_buffer_long, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
		if(myrank == 0){
			std::cout << "Write \t" + std::to_string(readWriteFrac[0]/1000000) + " MB total, request size \t" +std::to_string(readWriteFrac[1]/1000) + " KB: \t wrote \t" + std::to_string(PACKAGE_SIZE/1000000) + " MB at \t" + std::to_string(res_buffer_long/(size*1048576)) + " MiB/s" << std::endl;  
		}
		init_buffer_long = readWriteFrac[3];
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Reduce(&init_buffer_long, &res_buffer_long, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
		if(myrank == 0){
			std::cout << "Read \t" + std::to_string(readWriteFrac[0]/1000000) + " MB total, request size \t" +std::to_string(readWriteFrac[1]/1000) + " KB: \t  read \t" + std::to_string(PACKAGE_SIZE/1000000) + " MB at \t" + std::to_string(res_buffer_long/(size*1048576)) + " MiB/s" << std::endl;  
		}
	}
	MPI_Finalize();
	return 0;
}

	

