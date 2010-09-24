package Qudo::Parallel::Worker;
use strict;
use warnings;
use base 'Qudo::Worker';
our $VERSION = '0.01';

sub add_child_job {
    my ($self, $funcname, $arg) = @_;

    push @{$self->{_child_jobs}}, +{
        funcname => $funcname,
        arg      => $arg,
    };
}

sub start_child_job {
    my ($self, $args) = @_;

    my $job = $args->{job} or die "missing main job...";
    my $watch_dog = $args->{watch_dog} or die "missing watch dog class...";

    my $manager = $job->manager;
    my $db = $manager->driver_for($job->db);
    $db->dbh->begin_work;

    my @child_jobs;

        for my $child_job (@{$self->{_child_jobs}}) {
            $child_job->{arg}->{uniqkey} = join '_', $child_job->{funcname}, $job->id;
            push @child_jobs, $manager->enqueue($child_job->{funcname}, $child_job->{arg}, $db)->uniqkey;
        }

    $manager->enqueue($watch_dog, {arg => join ',', @child_jobs});

    $db->dbh->commit;
}

sub work_safely {
    my ($class, $job) = @_;

    my $self = bless +{}, $class;

    $job->job_start_time = time;

    my $res;
    eval {
        $res = $self->work($job);
    };

    if ($job->is_aborted) {
        $job->dequeue;
        return $res;
    }

    if ( my $e = $@ || ! $job->is_completed ) {
        if ( $job->retry_cnt < $class->max_retries ) {
            $job->reenqueue(
                {
                    grabbed_until => 0,
                    retry_cnt     => $job->retry_cnt + 1,
                    retry_delay   => $class->retry_delay,
                }
            );
        } else {
            $job->dequeue;
        }
        $job->failed($e || 'Job did not explicitly complete or fail');
    } else {
        $job->dequeue;
    }

    return $res;
}

1;
__END__

=head1 NAME

Qudo::Parallel::Worker -

=head1 SYNOPSIS

  use Qudo::Parallel::Worker;

=head1 DESCRIPTION

Qudo::Parallel::Worker is

=head1 AUTHOR

Atsushi Kobayashi E<lt>nekokak _at_ gmail _dot_ comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
