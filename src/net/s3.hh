/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#ifndef S3_HH
#define S3_HH

#include <ctime>
#include <string>
#include <vector>
#include <map>
#include <functional>
#include <fcntl.h>
#include <sys/types.h>

#include "socket.hh"
#include "secure_socket.hh"
#include "http_response_parser.hh"
#include "aws.hh"
#include "http_request.hh"
#include "requests.hh"
#include "util/path.hh"
#include "util/optional.hh"

// const static std::string UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD";
class S3
{
public:
  static std::string endpoint( const std::string & region,
                               const std::string & bucket );
};

class S3PutRequest : public AWSRequest
{
public:
  S3PutRequest( const AWSCredentials & credentials,
                const std::string & endpoint, const std::string & region,
                const std::string & object, const std::string & contents,
                const std::string & content_hash = {} );
};

class S3GetRequest : public AWSRequest
{
public:
  S3GetRequest( const AWSCredentials & credentials,
                const std::string & endpoint, const std::string & region,
                const std::string & object );
};

struct S3ClientConfig
{
  std::string region { "us-west-1" };
  std::string endpoint {};
  size_t max_threads { 32 };
  size_t max_batch_size { 32 };
  uint16_t port { 443 };
};

class S3Client
{
private:
  AWSCredentials credentials_;
  S3ClientConfig config_;

  /* template<typename sock_type>
  void do_upload_files(
    const std::vector<storage::PutRequest> & upload_requests,
    const size_t & thread_count,
    const size_t & batch_size,
    const size_t & first_file_idx,
    const sock_type & s3,
    const std::string & endpoint,
    const std::string & bucket,
    const std::string & region )
  {
    // SSLContext ssl_context;
    HTTPResponseParser responses;
    // SecureSocket s3 = ssl_context.new_secure_socket( tcp_connection( s3_address ) );

    // s3.connect();

    for ( size_t file_id = first_file_idx;
          file_id < std::min( upload_requests.size(), first_file_idx + thread_count * batch_size );
          file_id += thread_count ) {
      const std::string & filename = upload_requests.at( file_id ).filename.string();
      const std::string & object_key = ( region == "minio" ? bucket + "/" : "" ) + upload_requests.at( file_id ).object_key;
      std::string hash = upload_requests.at( file_id ).content_hash.get_or( UNSIGNED_PAYLOAD );

      std::string contents;
      FileDescriptor file { CheckSystemCall( "open " + filename, open( filename.c_str(), O_RDONLY ) ) };
      while ( not file.eof() ) { contents.append( file.read() ); }
      file.close();

      S3PutRequest request { credentials_, endpoint, config_.region,
                              object_key, contents, hash };

      HTTPRequest outgoing_request = request.to_http_request();
      responses.new_request_arrived( outgoing_request );

      s3.write( outgoing_request.str() );
    }

    size_t response_count = 0;

    while ( responses.pending_requests() ) {
      responses.parse( s3.read() );
      if ( not responses.empty() ) {
        if ( responses.front().first_line() != "HTTP/1.1 200 OK" ) {
          throw runtime_error( "HTTP failure in S3Client::upload_files(): " + responses.front().first_line() );
        }
        else {
          const size_t response_index = first_file_idx + response_count * thread_count;
          success_callback( upload_requests[ response_index ] );
        }

        responses.pop();
        response_count++;
      }
    }
  }

  template<typename sock_type>
  void do_download_files(
    const std::vector<storage::GetRequest> & download_requests,
    const size_t & thread_count,
    const size_t & batch_size,
    const size_t & first_file_idx,
    const sock_type & s3,
    const std::string & endpoint,
    const std::string & bucket,
    const std::string & region )
  {
    HTTPResponseParser responses;
    size_t expected_responses = 0;

    for ( size_t file_id = first_file_idx;
          file_id < std::min( download_requests.size(), first_file_idx + thread_count * batch_size );
          file_id += thread_count ) {
      const std::string & object_key = ( region == "minio" ? bucket + "/" : "" ) + download_requests.at( file_id ).object_key;

      S3GetRequest request { credentials_, endpoint, config_.region, object_key };

      HTTPRequest outgoing_request = request.to_http_request();
      responses.new_request_arrived( outgoing_request );

      s3.write( outgoing_request.str() );
      expected_responses++;
    }

    size_t response_count = 0;

    while ( response_count != expected_responses ) {
      responses.parse( s3.read() );
      if ( not responses.empty() ) {
        if ( responses.front().first_line() != "HTTP/1.1 200 OK" ) {
          const size_t response_index = first_file_idx + response_count * thread_count;
          throw runtime_error( "HTTP failure in downloading '" +
                                download_requests.at( response_index ).object_key +
                                "': " + responses.front().first_line() );
        }
        else {
          const size_t response_index = first_file_idx + response_count * thread_count;
          const std::string & filename = download_requests.at( response_index ).filename.string();

          roost::atomic_create( responses.front().body(), filename,
                                download_requests[ response_index ].mode.initialized(),
                                download_requests[ response_index ].mode.get_or( 0 ) );

          success_callback( download_requests[ response_index ] );
        }

        responses.pop();
        response_count++;
      }
    }
  } */

public:
  S3Client( const AWSCredentials & credentials,
            const S3ClientConfig & config = {} );

  void download_file( const std::string & bucket,
                      const std::string & object,
                      const roost::path & filename );

  void upload_files( const std::string & bucket,
                     const std::vector<storage::PutRequest> & upload_requests,
                     const std::function<void( const storage::PutRequest & )> & success_callback
                       = []( const storage::PutRequest & ){} );

  void download_files( const std::string & bucket,
                       const std::vector<storage::GetRequest> & download_requests,
                       const std::function<void( const storage::GetRequest & )> & success_callback
                         = []( const storage::GetRequest & ){} );
};

#endif /* S3_HH */
