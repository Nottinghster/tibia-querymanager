#if DATABASE_MARIADB
#include "querymanager.hh"

// TODO(fusion): If we decide to implement MySQL, we should use MariaDB instead.
// It is a better alternative overall, and targeting a single database system
// should help us leverage its strongest features, which may not always be
// supported by both.
#error "MariaDB is not currently supported."

#endif //DATABASE_MARIADB
