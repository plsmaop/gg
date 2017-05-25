#include "gg-model-base.hh"
#include "thunk_writer.hh"

#include <cstdlib>

using namespace std;
using namespace gg::thunk;

const char *GGModelBase::GG_DIR_FLAG = "GG_DIR";

string safe_getenv(const char *flag){
  char * var = getenv( flag );
  if( var == NULL ) {
    throw runtime_error("You must specify a GG directory" );
  }
  return string(var);
}

GGModelBase::GGModelBase() :
  GG_DIR( safe_getenv(GG_DIR_FLAG) )
{}

GGModelBase::~GGModelBase(){}

Thunk GGModelBase::build_thunk() {

  Function thunk_func = get_function();
  vector<InFile> infiles = get_infiles();
  string outfile = get_outfile();
  Thunk thunk { outfile, thunk_func, infiles };

  return thunk;
}

void GGModelBase::write_thunk() {

  Thunk thunk = build_thunk();
  ThunkWriter::write_thunk( thunk );

}

