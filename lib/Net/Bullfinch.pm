package Net::Bullfinch;
use Moose;
use MooseX::Params::Validate;
use MooseX::Types::DateTime;

# ABSTRACT: Perl wrapper for talking with Bullfinch

use Data::UUID;
use JSON::XS;
use Memcached::Client;

use Net::Bullfinch::Iterator;

=head1 DESCRIPTION

Net::Bullfinch is a thin wrapper around L<Memcached::Client> for communicating with
a L<Bullfinch|https://github.com/gphat/bullfinch/>.

This module handles JSON encoding of the request, the addition of a response
queue, waiting for a response, confirmation of the message, decoding of the
response and deletion of the response queue.

If you're expecting large numbers of results you might enjoy using
L<Net::Bullfinch::Iterator> to return any given number of max_results at a time.

=head1 SYNOPSIS

    use Net::Bullfinch;

    my $client = Net::Bullfinch->new(host => '172.16.49.130');
    my $req = { statement => 'some-query' };
    my $items = $client->send(
        request_queue => 'test-net-kestrel',
        request => $req,
        response_queue_suffix => 'foobar'
    );
    foreach my $item (@{ $items }) {
        # whatever
    }

=begin :prelude

=head1 TRACING

Bullfinch supports adding a tracing a request so that performance metrics
and responses can be tracked.

    my $items = $client->send(
        request_queue => 'test-net-kestrel',
        request => $req,
        response_queue_suffix => 'foobar'
        trace => 1
    );

A UUID will be added to the request.  This UUID will be included in the
performance code in bullfinch workers and included in the response you
receive.

=end :prelude

=cut

has '_client' => (
    is => 'rw',
    isa => 'Memcached::Client',
    default => sub {
        my $self = shift;
        return Memcached::Client->new ({
            servers => [ $self->host.':'.$self->port ]
        });
    },
    lazy => 1
);

=attr host

The IP address of the host that we'll be connecting to.

=cut
has 'host' => (
    is => 'rw',
    isa => 'Str',
    required => 1
);

=attr port

The port of the IP address of the host we'll be connecting to.

=cut
has 'port' => (
    is => 'rw',
    isa => 'Int',
    default => '22133'
);

=attr response_prefix

The prefix used for the name of the response queue.

=cut
has 'response_prefix' => (
    is => 'rw',
    isa => 'Str',
    default => 'response-net-kestrel-'
);

=attr timeout

Set the timeout (in milliseconds) that will be used when awaiting a response
back from Bullfinch.

=cut
has 'timeout' => (
    is => 'rw',
    isa => 'Int',
    default => 30000
);

=method send( request_queue => $queue, request => \%data, response_queue_suffix => $response_name, process_by => $procby, expiration => $expire);

Send the request to the specified queue and await a response.  The data
should be a hashref and the queuename (optional) will be appended to
C<response_prefix>.  This allows you to create a unique response queue per
request.

    # Response queue will be "response-net-kestrel-foobar"
    my $items = $client->send(\%data, "foobar");

Any messages sent in response (save the EOF message) are returned as an
arrayref to the caller.

The optional C<process_by> must be an ISO 8601 date.

The optional C<expiration> is the number of seconds this request should live
in the queue before expiring.

B<Note:> Send will die if it fails to properly enqueue the request.

=cut

sub send {
    my ($self, $queue, $data, $queuename, $trace, $procby, $expire) = validated_list(\@_,
        request_queue         => { isa => 'Str' },
        request               => { isa => 'HashRef' },
        response_queue_suffix => { isa => 'Str', optional => 1 },
        trace                 => { isa => 'Bool', default => 0, optional => 1 },
        process_by            => { isa => 'DateTime', optional => 1 },
        expiration            => { isa => 'Int', optional => 1 }
    );

    my ($rname, $json) = $self->_prepare_request($data, $queuename, $trace, $procby);
    my $kes = $self->_client;

    my $src = $kes->set($queue, $json, $expire);
    die "Failed to send request!" unless $src;

    my @items = ();
    while(1) {
        my $resp = $kes->get($rname.'/t='.$self->timeout);
        if(defined($resp)) {
            my $decoded = decode_json($resp);
            if(exists($decoded->{EOF})) {
                last;
            }
            push(@items, $decoded);
        }

        if(!defined($resp)) {
            last;
        }
    }
    my $drc = $kes->delete($rname);
    warn "Failed to delete response queue!" unless $drc;

    return \@items;
}

sub iterate {
    my ($self, $queue, $data, $queuename, $iterator_options) = validated_list(\@_,
        request_queue         => { isa => 'Str' },
        request               => { isa => 'HashRef' },
        response_queue_suffix => { isa => 'Str', optional => 1 },
        iterator_options      => { isa => 'HashRef', optional => 1 }
    );

    my ($rname, $json) = $self->_prepare_request($data, $queuename);
    my $kes = $self->_client;

    $kes->set($queue, $json);

    Net::Bullfinch::Iterator->new(
        bullfinch      => $self,
        response_queue => $rname,
        %$iterator_options
    );
}

sub _prepare_request {
    my ($self, $data, $queuename, $trace, $procby) = @_;

    # Make a copy of the hash so that we can add a key to it
    my %copy = %{ $data };

    my $rname = $self->response_prefix;
    if(defined($queuename)) {
        $rname .= $queuename
    }

    $copy{response_queue} = $rname;

    # User requested a trace, generate one
    if($trace) {
        my $ug = Data::UUID->new;
        $copy{tracer} = $ug->create_str;
    }
    
    if($procby) {
        $copy{'process-by'} = $procby->iso8601;
    }

    return ($rname, encode_json(\%copy));
}

1;
