/* -*-mode:c++; tab-width: 2; indent-tabs-mode: nil; c-basic-offset: 2 -*- */

#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <stack>
#include <vector>

#include "thunk/factory.hh"
#include "util/exception.hh"
#include "util/tokenize.hh"
#include "thunk/ggutils.hh"

using namespace std;

void parse()
{
  std::string function_hash; /* if not specified, it's the first executable */
  deque<string> args;
  deque<ThunkFactory::Data> data;
  deque<ThunkFactory::Data> executables;
  deque<ThunkFactory::Output> outputs;
  deque<pair<string, string>> links;

  for ( string line; getline( cin, line ); ) {
    const auto tokens = split( line, " " );

    if ( tokens.empty() ) continue;
    
    if ( tokens[0] == "push" ) {
      if ( tokens.size() < 3 ) {
        throw runtime_error( "'push' needs at least two arguments" );
      }

      switch ( tokens[1][0] ) {
      case 'a': args.push_front( tokens[2] ); break;
      case 'd': data.emplace_front( tokens[2] ); break;
      case 'e': executables.emplace_front( tokens[2] ); break;
      case 'o': outputs.emplace_front( tokens[2] ); break;
      case 'l': links.emplace_front( tokens[2], gg::hash::file( tokens.at( 3 ) ) ); break;
      
      default: throw runtime_error( "invalid push operation: " + tokens[1] );
      }
    }
    else if ( tokens[0] == "pop" ) {
      if ( tokens.size() != 2 ) {
        throw runtime_error( "'pop' needs one argument" );
      }

      switch ( tokens[1][0] ) {
      case 'a': args.pop_front(); break;
      case 'd': data.pop_front(); break;
      case 'e': executables.pop_front(); break;
      case 'o': outputs.pop_front(); break;
      case 'l': links.pop_front(); break;
      
      default: throw runtime_error( "invalid pop operation: " + tokens[1] );
      }
    }
    else if ( tokens[0] == "set" ) {
      if ( tokens.size() != 3 ) {
        throw runtime_error( "'pop' needs two arguments" );
      }

      switch ( tokens[1][0] ) {
      case 'f': function_hash = tokens[2]; break;
      default: runtime_error( "invalid set operation: " + tokens[1] );
      }
    }
    else if ( tokens[0] == "clear" ) {
      function_hash.clear();
      args.clear();
      data.clear();
      executables.clear();
      outputs.clear();
      links.clear();
    }
    else if ( tokens[0] == "create" ) {
      if ( tokens.size() != 1 ) {
        throw runtime_error( "'create' has no arguments" );
      }

      map<string, string> links_map;
      for ( auto & link : links ) {
        links_map[link.first] = link.second;
      }

      const string thunk_hash = ThunkFactory::generate<deque>( 
        { function_hash.empty() ? executables.at( 0 ).hash() : function_hash,
          { args.begin(), args.end() },
          {} },
        data,
        executables,
        outputs,
        {},
        0ms,
        ThunkFactory::Options::collect_data,
        links_map );

      cout << "+" << thunk_hash << endl;
    }
    else {
      throw runtime_error( "invalid command: " + tokens[0] );
    }
  }
}

void usage( char * argv0 )
{
  cerr << "Usage: " << argv0 << endl;
}

int main( int argc, char * argv[] )
{
  try {
    if ( argc <= 0 ) {
      abort();
    }

    if ( argc != 1 ) {
      usage( argv[ 0 ] );
      return EXIT_FAILURE;
    }

    parse();
  }
  catch ( const exception & ex ) {
    print_exception( argv[ 0 ], ex );
    return EXIT_FAILURE;
  }

  return EXIT_SUCCESS;
}
