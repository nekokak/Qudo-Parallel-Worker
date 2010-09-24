use Qudo::Test;
use Test::More;
use Test::Output;

run_tests(4, sub {
    my $driver = shift;
    my $master = test_master(
        driver_class => $driver,
    );

    my $manager = $master->manager;
    $manager->register_abilities(qw/Worker::Main Worker::Child1 Worker::Child2 Worker::Watchdog/);

    $manager->enqueue("Worker::Main", {});
    $manager->work_once;

    my $row = $master->job_count;
    my ($dsn, $count) = each %$row;
    is $count, 3;

    stdout_is( sub { $manager->work_once } , 'Worker::Child1 worked');
    stdout_is( sub { $manager->work_once } , 'Worker::Child2 worked');
    stdout_is( sub { $manager->work_once } , 'all worker completed');

    teardown_dbs;
});

package Worker::Main;
use base 'Qudo::Parallel::Worker';
sub work {
    my ($self, $job) = @_;

    $self->add_child_job("Worker::Child1", { arg => 'arg', priority => 10});
    $self->add_child_job("Worker::Child2", { arg => 'arg', priority => 5});
    $self->start_child_job(
        {
            job       => $job,
            watch_dog => 'Worker::Watchdog',
        }
    );

    $job->is_completed;
}

package Worker::Child1;
use base 'Qudo::Worker';
sub set_job_status {1}
sub work {
    my ($class, $job) = @_;
    print STDOUT 'Worker::Child1 worked';
    $job->is_completed;
}

package Worker::Child2;
use base 'Qudo::Worker';
sub set_job_status {1}
sub work {
    my ($class, $job) = @_;
    print STDOUT 'Worker::Child2 worked';
    $job->is_completed;
}

package Worker::Watchdog;
use base 'Qudo::Parallel::Worker::Watchdog';
sub work {
    my ($class, $job) = @_;
    print STDOUT 'all worker completed';
}

