/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef STORAGE_BACKEND_S3_HH
#define STORAGE_BACKEND_S3_HH

#include <chrono>
#include <iostream>

#include "backend.hh"
#include "net/aws.hh"
#include "net/s3.hh"

using namespace std::chrono;

class S3StorageBackend : public StorageBackend
{
private:
  S3Client client_;
  std::string bucket_;
  uint16_t port_;
  std::chrono::duration<double, std::micro> duration_ = std::chrono::duration<double, std::micro>(0);

public:
  S3StorageBackend( const AWSCredentials & credentials,
                    const std::string & s3_bucket,
                    const std::string & s3_region,
                    const uint16_t & port );
  
  ~S3StorageBackend() {
    auto duration = duration_cast<microseconds>(duration_);
    std::cout << duration.count() << std::endl;
  }

  void put( const std::vector<storage::PutRequest> & requests,
            const PutCallback & success_callback = []( const storage::PutRequest & ){} ) override;

  void get( const std::vector<storage::GetRequest> & requests,
            const GetCallback & success_callback = []( const storage::GetRequest & ){} ) override;

};

#endif /* STORAGE_BACKEND_S3_HH */
