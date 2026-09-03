#!/usr/bin/perl
#
# Copyright SUSE LLC
# SPDX-License-Identifier: GPL-2.0-or-later

use Test::Most;
use Mojo::Base -signatures;
use Test::MockModule 'strict';
use FindBin '$Bin';
use lib "$Bin/../external/os-autoinst-common/lib";
use OpenQA::Test::TimeLimit '5';
use Socket;

# mock sysread to be able to limit the number of bytes read from the socket
my $mock_sysread_limit;

BEGIN {
    *CORE::GLOBAL::sysread = sub : prototype(*$;$$) {
        my ($socket, undef, $length, $offset) = @_;
        $length = $mock_sysread_limit if defined $mock_sysread_limit && $length > $mock_sysread_limit;
        return CORE::sysread($socket, $_[1], $length, $offset // 0);
    };
}

use myjsonrpc;

use Test::Warnings qw(warnings :report_warnings);

no warnings 'redefine';
*bmwqemu::diag = sub ($msg) { warn $msg };

my ($child, $isotovideo);
socketpair $child, $isotovideo, AF_UNIX, SOCK_STREAM, PF_UNSPEC;

$child->autoflush(1);
$isotovideo->autoflush(1);

my $send1 = {a => 1, umlaut => 'testä'};
my $send2 = {b => 12, json_cmd_token => 'dummy'};

subtest single_json => sub {
    myjsonrpc::send_json($child, $send1);
    my $read = myjsonrpc::read_json($isotovideo);
    ok exists $read->{json_cmd_token}, 'send_json/read_json json_cmd_token exists';
    delete $read->{json_cmd_token};

    is_deeply $read, $send1, 'read_json returns what send_json sent';
    $send1->{json_cmd_token} = 'dummy';

    myjsonrpc::send_json($child, $send1);
    myjsonrpc::send_json($child, $send2);
    my $read1 = myjsonrpc::read_json($isotovideo, undef, 0);
    my $read2 = myjsonrpc::read_json($isotovideo, undef, 0);
    is_deeply [$read1, $read2], [$send1, $send2], 'read_json twice works';
};

subtest multi_json => sub {
    myjsonrpc::send_json($child, $send1);
    myjsonrpc::send_json($child, $send2);
    my @read = myjsonrpc::read_json($isotovideo, undef, 1);
    is_deeply \@read, [$send1, $send2], 'read_json in list context works';
};

sub magic_close () {
    myjsonrpc::send_json($child, {QUIT => 1});
    my $quit = myjsonrpc::read_json($isotovideo);
    is $quit, undef, 'received magic close';
}

subtest magic_close => sub {
    my @warnings = warnings { magic_close() };
    like $warnings[0], qr{received magic close};
};

subtest 'send_json dies when buffer is empty and pipe is broken' => sub {
    my $myjsonrpc_mock = Test::MockModule->new('myjsonrpc');
    $myjsonrpc_mock->redefine(_syswrite => sub ($to_fd, $json, $len, $offset) { 0 });
    dies_ok { myjsonrpc::send_json($child, $send1) } 'myjsonrpc: remote end terminated connection, stopping';
};

subtest 'handling interleaved commands' => sub {
    my ($sub_child, $sub_isotovideo);
    socketpair $sub_child, $sub_isotovideo, AF_UNIX, SOCK_STREAM, PF_UNSPEC;
    $sub_child->autoflush(1);
    $sub_isotovideo->autoflush(1);

    my $msg_a = {a => 1, json_cmd_token => 'token-A'};
    my $msg_b = {b => 2, json_cmd_token => 'token-B'};
    my $msg_c = {c => 3, json_cmd_token => 'token-C'};
    my $msg_d = {d => 4, json_cmd_token => 'token-D'};

    subtest 'interleaved_command_handler is not set' => sub {
        myjsonrpc::send_json($sub_child, $msg_c);
        myjsonrpc::send_json($sub_child, $msg_d);
        myjsonrpc::send_json($sub_child, $msg_b);
        myjsonrpc::send_json($sub_child, $msg_a);
        my $read_a = myjsonrpc::read_json($sub_isotovideo, 'token-A');
        is_deeply $read_a, $msg_a, 'read_json with token-A returns msg_a';
        my $read_b = myjsonrpc::read_json($sub_isotovideo, 'token-B');
        is_deeply $read_b, $msg_b, 'subsequent read_json call returns msg_b from cached results';
        my @read_c_d = myjsonrpc::read_json($sub_isotovideo, undef, 1);
        is_deeply \@read_c_d, [$msg_c, $msg_d], 'subsequent multi read_json call returns msg_c and msg_d from cached results';
    };
    subtest 'interleaved_command_handler is set' => sub {
        my @interleaved;
        myjsonrpc::set_interleaved_command_handler(\@interleaved);
        myjsonrpc::send_json($sub_child, $msg_b);
        myjsonrpc::send_json($sub_child, $msg_a);
        my $read_a = myjsonrpc::read_json($sub_isotovideo, 'token-A');
        is_deeply $read_a, $msg_a, 'read_json with token-A returns msg_a';
        is_deeply \@interleaved, [[$msg_b, $sub_isotovideo]], 'message msg_b captured';
    };

    myjsonrpc::set_interleaved_command_handler(undef);
    close $sub_child;
    close $sub_isotovideo;
};

subtest 'reproduce deadlock/newline issue' => sub {
    my ($child, $isotovideo);
    socketpair $child, $isotovideo, AF_UNIX, SOCK_STREAM, PF_UNSPEC;
    $child->autoflush(1);
    $isotovideo->autoflush(1);

    # send a test message; it is not supposed to contain a trailing newline
    my $cjx = Cpanel::JSON::XS->new->canonical->utf8->convert_blessed();
    my $json_str = $cjx->encode({test => 'deadlock', json_cmd_token => '12345678'});
    my $json_len = length $json_str;
    myjsonrpc::send_json($child, {test => 'deadlock', json_cmd_token => '12345678'});

    # mock IO::Select::can_read to keep track of whether it would run into a deadlock
    my $can_read_count = 0;
    my $test_io_select_mock = Test::MockModule->new('IO::Select');
    $test_io_select_mock->redefine(can_read => sub ($self, $timeout = undef) {
            my $original_can_read = $test_io_select_mock->original('can_read');
            die 'deadlock detected: can_read called multiple times because of trailing newline' if ++$can_read_count > 1;    # uncoverable statement
            return $original_can_read->($self, $timeout);
    });

    # read with sysread limited to read exactly the JSON length, leaving any trailing newline in the socket
    # note: In practice, this can happen if the JSON length matches exactly the size of the pipe.
    $mock_sysread_limit = $json_len;
    my $read = myjsonrpc::read_json($isotovideo, undef);
    is_deeply $read, {test => 'deadlock', json_cmd_token => '12345678'}, 'parsed JSON object';
    my $bytes_left_to_read = $test_io_select_mock->original('can_read')->(IO::Select->new($isotovideo), 0);
    ok !$bytes_left_to_read, 'socket is not readable; no leftover trailing newline in socket';

    # call read_json again as the consumer would do if there is a leftover newline to read
    myjsonrpc::read_json($isotovideo, undef) if $bytes_left_to_read;    # uncoverable statement

    undef $mock_sysread_limit;
    close $child;
    close $isotovideo;
};

my $io_select_mock = Test::MockModule->new('IO::Select');
$io_select_mock->redefine(can_read => undef);
throws_ok { myjsonrpc::read_json($isotovideo) } qr/Illegal seek/, 'error exception raised when reading is aborted';

close $isotovideo;
close $child;

done_testing;
