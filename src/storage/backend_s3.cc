/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include "backend_s3.hh"

using namespace std;
using namespace storage;

S3StorageBackend::S3StorageBackend( const AWSCredentials & credentials,
                                    const string & s3_bucket,
                                    const string & s3_region,
                                    const uint16_t & port )
  : client_( credentials, { s3_region, "", 32, 32, port } ), bucket_( s3_bucket ), port_( port )
{}

void S3StorageBackend::put( const std::vector<PutRequest> & requests,
                            const PutCallback & success_callback )
{
  auto start = high_resolution_clock::now();
  client_.upload_files( bucket_, requests, success_callback );
  auto stop = high_resolution_clock::now();

  duration_ += (stop - start);
}

void S3StorageBackend::get( const std::vector<GetRequest> & requests,
                            const GetCallback & success_callback )
{
  auto start = high_resolution_clock::now();
  client_.download_files( bucket_, requests, success_callback );
  auto stop = high_resolution_clock::now();

  duration_ += (stop - start);
}
