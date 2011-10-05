package Net::Bullfinch;
use Moose;
use MooseX::Params::Validate;

# ABSTRACT: Perl wrapper for talking with Bullfinch

use Data::UUID;
use JSON::XS;
use Net::Kestrel;

use Net::Bullfinch::Iterator;

=head1 DESCRIPTION

Net::Bullfinch is a thin wrapper around <Net::Kestrel> for communicating with
a L<Bullfinch|https://github.com/gphat/bullfinch/>.

This module handles JSON encoding of the request, the addition of a response
queue, waiting for a response, confirmation of the message, decoding of the
response and deletion of the response queue.

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

=cut

has '_client' => (
    is => 'rw',
    isa => 'Net::Kestrel',
    default => sub {
        my $self = shift;
        return Net::Kestrel->new(host => $self->host);
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

=method send( request_queue => $queue, request => \%data, response_queue_suffix => $response_name);

Send the request to the specified queue and await a response.  The data
should be a hashref and the queuename (optional) will be appended to
C<response_prefix>.  This allows you to create a unique response queue per
request.

    # Response queue will be "response-net-kestrel-foobar"
    my $items = $client->send(\%data, "foobar");

Any messages sent in response (save the EOF message) are returned as an
arrayref to the caller.

=cut

sub send {
    my ($self, $queue, $data, $queuename, $trace) = validated_list(\@_,
        request_queue         => { isa => 'Str' },
        request               => { isa => 'HashRef' },
        response_queue_suffix => { isa => 'Str', optional => 1 },
        trace                 => { isa => 'Bool', default => 0, optional => 1 }
    );

    my ($rname, $json) = $self->_prepare_request($data, $queuename, $trace);
    my $kes = $self->_client;

    $kes->put($queue, $json);

    my @items = ();
    while(1) {
        my $resp = $kes->get($rname, $self->timeout);
        if(defined($resp)) {
            $kes->confirm($rname, 1);
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
    $kes->delete($rname);

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

    $kes->put($queue, $json);

    Net::Bullfinch::Iterator->new(
        bullfinch      => $self,
        response_queue => $rname,
        %$iterator_options
    );
}

sub _prepare_request {
    my ($self, $data, $queuename, $trace) = @_;

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

    return ($rname, encode_json(\%copy));
}

1;
