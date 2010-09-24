package Qudo::Parallel::Worker::Watchdog;
use strict;
use warnings;
use base 'Qudo::Worker';

sub max_retries { 10 }
sub retry_delay { 10 }

sub work_safely {
    my ($class, $job) = @_;

    if ($job->funcname->set_job_status) {
        $job->job_start_time = time;
    }

    my $master = $job->manager->{qudo};

    my @uniqkeys = split ',', $job->arg;
    my @funcs;
    for my $key (@uniqkeys) {
        my ($func, undef) = split '_', $key;
        push @funcs, $func;
    }
    my $args = +{
        limit  => 0,
        offset => 0,
        funcs  => \@funcs,
    };
    my $rows = $master->driver_for($job->db)->job_status_list($args);

    unless (scalar(@$rows)) {
        $job->reenqueue(
            {
                grabbed_until => 0,
                retry_cnt     => $job->retry_cnt + 1,
                retry_delay   => $class->retry_delay,
            }
        );
        return;
    }

    my @ok_uniqkeys;
    for my $row (@$rows) {
        for my $uniqkey (@uniqkeys) {
            if ($row->{uniqkey} eq $uniqkey) {
                push @ok_uniqkeys, $uniqkey;
            }
        }
    }
    if (scalar(@ok_uniqkeys)!=scalar(@uniqkeys)) {
        $job->reenqueue(
            {
                grabbed_until => 0,
                retry_cnt     => $job->retry_cnt + 1,
                retry_delay   => $class->retry_delay,
            }
        );
        return;
    }

    my $res;
    eval {
        $res = $class->work($job);
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
