use inc::Module::Install;
name 'Qudo-Parallel-Worker';
all_from 'lib/Qudo/Parallel/Worker.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
