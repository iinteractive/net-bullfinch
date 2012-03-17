package Net::Bullfinch::Iterator;
use Moose;

# ABSTRACT: A way to iterator over results from Bullfinch

use JSON::XS;
use Try::Tiny;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);

use Net::Bullfinch;

with 'Data::Stream::Bulk::DoneFlag';

=head1 DESCRIPTION

This module provides a way to iterate over a result stream
coming from Net::Bullfinch. It uses the Data::Stream::Bulk
role and so therefore has all the functionality implied therein.

=head1 SYNOPSIS

    use Net::Bullfinch;

    my $client = Net::Bullfinch->new(host => '172.16.49.130');
    my $req = { statement => 'some-query' };
    my $items = $client->iterate(
        request_queue => 'test-net-kestrel',
        request => $req,
        response_queue_suffix => 'foobar',
        iterator_options => { max_results => 200 }
    );

    while ( my $block = $items->next ) {
        foreach my $item ( @$block ) {
            # do something with each item ...
        }
    }

=cut

has 'bullfinch' => (
    is       => 'ro',
    isa      => 'Net::Bullfinch',
    required => 1
);

has 'response_queue' => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

has 'max_results' => (
    is      => 'ro',
    isa     => 'Int',
    default => 25,
);

sub get_more {
    my $self = shift;

    my $kestrel    = $self->bullfinch->_client;
    my $resp_queue = $self->response_queue;
    my $timeout    = $self->bullfinch->timeout;

    my @results;

    while ( 1 ) {
        my $resp = $kestrel->get($resp_queue.'/t='.$timeout);

        if ( defined $resp ) {
            my $decoded = $self->decode( $resp );
            if ( exists $decoded->{'EOF'} ) {
                $self->_set_done;
                last;
            }
            push @results => $decoded;
        }

        last if scalar @results == $self->max_results;

        if (not defined $resp) {
            $self->_set_done;
            last;
        }
    }

    return \@results;
}

sub all {
    my $self = shift;

    my $kestrel    = $self->bullfinch->_client;
    my $resp_queue = $self->response_queue;
    my $timeout    = $self->bullfinch->timeout;

    my @results;

    while ( 1 ) {
        my $resp = $kestrel->get( $resp_queue.'/t='.$timeout );

        if ( defined $resp ) {
            my $decoded = $self->decode( $resp );
            if ( exists $decoded->{'EOF'} ) {
                last;
            }
            push @results => $decoded;
        }

        last if not defined $resp;
    }

    $self->_set_done;

    return \@results;
}

sub finished {
    my $self = shift;
    $self->bullfinch->_client->delete( $self->response_queue )
}

sub decode {
    my ($self, $in) = @_;
    my $decoded;
    try {
        $decoded = decode_json( $in );
    } catch {
        # Sometimes the payload is gzip'd...
        my $input = \$in;
        my $o = 0;
        my $output = \$o;
        no strict 'refs';
        my $status = gunzip $input => $output
            or die "gunzip failed: $GunzipError\n";
        $decoded = decode_json( $$output );
    };
    return $decoded;
}

sub loaded { 1 }

__PACKAGE__->meta->make_immutable;

1;



