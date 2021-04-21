/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <fcntl.h>
#include <thread>

#include "backend_local.hh"
#include "util/exception.hh"
#include "util/temp_file.hh"

using namespace std;
using namespace storage;

void LocalStorageBackend::put( const std::vector<PutRequest> & upload_requests,
                               const PutCallback & success_callback )
{
  size_t max_threads { 32 };
  size_t max_batch_size { 32 };

  vector<thread> threads;
  for ( size_t thread_index = 0; thread_index < max_threads; thread_index++ ) {
    if ( thread_index < upload_requests.size() ) {
      threads.emplace_back(
        [&] ( const size_t index )
        {
          for ( size_t first_file_idx = index;
                first_file_idx < upload_requests.size();
                first_file_idx += max_threads * max_batch_size ) {

            size_t expected_responses = 0;

            for ( size_t file_id = first_file_idx;
              file_id < min( upload_requests.size(), first_file_idx + max_threads * max_batch_size );
              file_id += max_threads ) {

              const string & filename = upload_requests.at( file_id ).filename.string();
              const string & object_key = upload_requests.at( file_id ).object_key;

              string contents;
              FileDescriptor file { CheckSystemCall( "open " + filename, open( filename.c_str(), O_RDONLY ) ) };
              while ( not file.eof() ) { contents.append( file.read() ); }
              file.close();

              // write
              string dst_path = path_ + "/" + object_key;
              FileDescriptor dst { CheckSystemCall( "open " + dst_path, open( dst_path.c_str(), O_RDWR ) ) };
              dst.write(contents, true);
              dst.close();
              expected_responses++;
            }

            size_t response_count = 0;

            while ( response_count != expected_responses ) {

              const size_t response_index = first_file_idx + response_count * max_threads;
              success_callback( upload_requests[ response_index ] );

              response_count++;
            }
          }
        }, thread_index
      );
    }
  }

  for ( auto & thread : threads ) {
    thread.join();
  }
}

void LocalStorageBackend::get( const std::vector<GetRequest> & download_requests,
                               const GetCallback & success_callback )
{
  size_t max_threads { 32 };
  size_t max_batch_size { 32 };

  vector<thread> threads;
  for ( size_t thread_index = 0; thread_index < max_threads; thread_index++ ) {
    if ( thread_index < download_requests.size() ) {
      threads.emplace_back(
        [&] ( const size_t index )
        {
          for ( size_t first_file_idx = index;
                first_file_idx < download_requests.size();
                first_file_idx += max_threads * max_batch_size ) {

            size_t response_count = 0;

            for ( size_t file_id = first_file_idx;
                  file_id < min( download_requests.size(), first_file_idx + max_threads * max_batch_size );
                  file_id += max_threads ) {
              const string & object_key = download_requests.at( file_id ).object_key;

              string contents;
              string src_path = path_ + "/" + object_key;
              FileDescriptor file { CheckSystemCall( "open " + src_path, open( src_path.c_str(), O_RDONLY ) ) };
              while ( not file.eof() ) { contents.append( file.read() ); }
              file.close();

              const size_t response_index = first_file_idx + response_count * max_threads;
              const string & filename = download_requests.at( response_index ).filename.string();

              roost::atomic_create( contents, filename,
                                    download_requests[ response_index ].mode.initialized(),
                                    download_requests[ response_index ].mode.get_or( 0 ) );

              success_callback( download_requests[ response_index ] );
              response_count++;
            }
          }
        }, thread_index
      );
    }
  }

  for ( auto & thread : threads ) {
    thread.join();
  }
}
