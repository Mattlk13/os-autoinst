# Copyright 2021 Marek Marczykowski-Górecki
# Copyright 2021 SUSE LLC
# SPDX-License-Identifier: GPL-2.0-or-later

package consoles::video_stream;

use Mojo::Base 'consoles::video_base', -signatures;
use Mojo::UserAgent;
use Mojo::URL;
use Mojo::Util 'scope_guard';

use Config;
use List::Util 'max';
use Time::HiRes qw(usleep clock_gettime CLOCK_MONOTONIC);
use Fcntl;
use File::Map qw(map_handle unmap);
use IPC::Open2 qw(open2);

use bmwqemu;

# speed limit: 30 keys per second
use constant STREAM_TYPING_LIMIT_DEFAULT => 30;

use constant DV_TIMINGS_CHECK_INTERVAL => 3;

use constant STALL_THRESHOLD => 4;

use constant DEFAULT_MAX_RES_X => 1680;
use constant DEFAULT_MAX_RES_Y => 1050;
use constant DEFAULT_MAX_RES => DEFAULT_MAX_RES_X * DEFAULT_MAX_RES_Y;
use constant DEFAULT_BYTES_PER_PIXEL => 3;
use constant DEFAULT_PPM_HEADER_BYTES => 20;
use constant DEFAULT_VIDEO_STREAM_PIPE_BUFFER_SIZE => DEFAULT_MAX_RES * DEFAULT_BYTES_PER_PIXEL + DEFAULT_PPM_HEADER_BYTES;

sub screen ($self, @) {
    return $self;
}

sub _stop_process ($self, $name) {
    return undef unless my $pipe = delete $self->{$name};
    my $pid = delete $self->{"${name}pid"};
    kill(TERM => $pid);
    close($pipe);
    return waitpid($pid, 0);
}

sub disable_video ($self) {
    my $ret = 0;
    $ret ||= $self->_stop_process('ffmpeg');
    $ret ||= $self->_stop_process('ustreamer');
    return $ret;
}

sub disable ($self, @) {
    my $ret = $self->disable_video;
    if ($self->{input_pipe}) {
        close($self->{input_pipe});
        close($self->{input_feedback});
        waitpid($self->{inputpid}, 0);
    }
    return $ret;
}

# uncoverable statement count:1..5 note:the function is redefined in tests
sub _v4l2_ctl ($device, $cmd_prefix, $cmd) {
    my @cmd = split(/ /, $cmd_prefix // '');    # uncoverable statement
    $device =~ s/\?fps=([0-9]+)//;    # uncoverable statement
    push(@cmd, ("v4l2-ctl", "--device", $device, "--concise"));    # uncoverable statement
    push(@cmd, split(/ /, $cmd));    # uncoverable statement

    # uncoverable statement
    my $pipe;
    # uncoverable statement
    my $pid = open($pipe, '-|', @cmd) or return undef;
    # uncoverable statement
    $pipe->read(my $str, 50);
    # uncoverable statement
    my $ret = waitpid($pid, 0);
    # uncoverable statement
    if ($ret > 0 && $? == 0) {
        # remove header and whitespaces
        $str =~ s/DV timings://;    # uncoverable statement
        $str =~ s/^\s+|\s+$//g;    # uncoverable statement
        return $str;    # uncoverable statement
    }
    return undef;    # uncoverable statement
}

sub connect_remote ($self, $args) {
    $self->{_last_update_received} = 0;

    if ($args->{url} =~ m/^(ustreamer:\/\/)?(\/dev\/video\d+)/) {
        if ($args->{edid}) {
            my $ret = _v4l2_ctl($2, $args->{video_cmd_prefix}, "--set-edid $args->{edid}");
            die "Failed to set EDID" unless defined $ret;
        }
    }

    if ($args->{url} =~ m/^\/dev\/video/) {
        my $timings = _v4l2_ctl($args->{url}, $args->{video_cmd_prefix}, '--get-dv-timings');
        if ($timings) {
            if ($timings ne "0x0pnan") {
                $self->{dv_timings} = $timings;
            } else {
                $self->{dv_timings} = '';
            }
            $self->{dv_timings_supported} = 1;
            $self->{dv_timings_last_check} = time;
            bmwqemu::diag "Current DV timings: $timings";
        } else {
            $self->{dv_timings_supported} = 0;
            bmwqemu::diag "DV timings not supported";
        }
    } else {
        # applies to v4l via ffmpeg only
        $self->{dv_timings_supported} = 0;
    }

    bmwqemu::diag "Starting to receive video stream at $args->{url}";
    $self->connect_remote_video($args->{url});

    $self->connect_remote_input($args->{input_cmd}) if $args->{input_cmd};
}

sub _get_ffmpeg_cmd ($self, $url) {
    my $fps = $1 if ($url =~ s/[\?&]fps=([0-9]+)//);
    die "ffmpeg url does not support format=" if ($url =~ s/[\?&]format=([A-Z0-9]+)//);
    $fps //= 4;
    my @cmd;
    @cmd = split(/ /, $self->{args}->{video_cmd_prefix}) if $self->{args}->{video_cmd_prefix};
    push(@cmd, ('ffmpeg', '-loglevel', 'fatal', '-i', $url));
    push(@cmd, ('-vcodec', 'ppm', '-f', 'rawvideo', '-r', $fps, '-'));
    return \@cmd;
}

sub _get_ustreamer_cmd ($self, $url, $sink_name) {
    my $fps = $1 if ($url =~ s/[\?&]fps=([0-9]+)//);
    my $format = $1 if ($url =~ s/[\?&]format=([A-Z0-9]+(swap)?)//);
    $fps //= 5;
    $format //= 'UYVY';
    my $swap = ($format =~ /swap$/);
    $format =~ s/swap$//;
    my $cmd = [
        'ustreamer', '--device', $url, '-f', $fps,
        '-m', $format,    # specify preferred format
        '-c', 'NOOP',    # do not produce JPEG stream
        '--raw-sink', $sink_name, '--raw-sink-rm',    # raw memsink
        '--persistent',    # smarter watching for reconnecting HDMI, and since ustreamer 6.0 - necessary for --dv-timings to work
        '--dv-timings',    # enable using DV timings (getting resolution, and reacting to changes)
    ];
    # workaround for https://github.com/raspberrypi/linux/issues/6068
    push(@$cmd, ('--format-swap-rgb', '1')) if ($swap);
    return $cmd;
}

sub connect_remote_video ($self, $url) {
    if ($self->{dv_timings_supported}) {
        if (!_v4l2_ctl($url, $self->{args}->{video_cmd_prefix}, '--set-dv-bt-timings query')) {
            bmwqemu::diag("No video signal");
            $self->{dv_timings} = '';
            return;
        }
        $self->{dv_timings} = _v4l2_ctl($url, $self->{args}->{video_cmd_prefix}, '--get-dv-timings');
    }

    if ($url =~ m^ustreamer://^) {
        die 'unsupported arch' unless ($Config{archname} =~ /^aarch64|x86_64/);
        my $dev = ($url =~ m^ustreamer://(.*)^)[0];
        my $sink_name = "raw-sink$dev.raw";
        $sink_name =~ s^/^-^g;
        my $cmd = $self->_get_ustreamer_cmd($dev, $sink_name);
        my $ffmpeg;
        $self->{ustreamerpid} = open($ffmpeg, '-|', @$cmd)
          or die "Failed to start ustreamer for video stream at $url";
        $self->{ustreamer_pipe} = $ffmpeg;
        my $timeout = 100;
        while ($timeout && !-f "/dev/shm/$sink_name") {
            usleep(100_000);    # uncoverable statement
            $timeout -= 1;    # uncoverable statement
        }
        die "ustreamer startup timeout" if $timeout <= 0;
        open($self->{ustreamer}, "+<", "/dev/shm/$sink_name")
          or die "Failed to open ustreamer memsink";
    } else {
        my $cmd = $self->_get_ffmpeg_cmd($url);
        my $ffmpeg;
        $self->{ffmpegpid} = open($ffmpeg, '-|', @$cmd)
          or die "Failed to start ffmpeg for video stream at $url";
        # make the pipe size large enough to hold full frame and a bit
        my $frame_size = $bmwqemu::vars{VIDEO_STREAM_PIPE_BUFFER_SIZE} // DEFAULT_VIDEO_STREAM_PIPE_BUFFER_SIZE;
        fcntl($ffmpeg, Fcntl::F_SETPIPE_SZ, $frame_size);
        $self->{ffmpeg} = $ffmpeg;
        $ffmpeg->blocking(0);
    }

    $self->{_last_update_received} = time;

    return 1;
}

sub connect_remote_input ($self, $cmd) {
    $self->{mouse} = {x => -1, y => -1};

    bmwqemu::diag "Connecting input device";

    my $input_pipe;
    my $input_feedback;
    $self->{inputpid} = open2($input_feedback, $input_pipe, $cmd)
      or die "Failed to start input_cmd($cmd)";
    $self->{input_pipe} = $input_pipe;
    $self->{input_pipe}->autoflush(1);
    $self->{input_feedback} = $input_feedback;

    return $input_pipe;
}


sub _receive_frame_ffmpeg ($self) {
    my $ffmpeg = $self->{ffmpeg};
    $ffmpeg or die 'ffmpeg is not running. Probably your backend instance could not start or died.';
    $ffmpeg->blocking(0);
    my $ret = $ffmpeg->read(my $header, DEFAULT_PPM_HEADER_BYTES);
    $ffmpeg->blocking(1);

    return undef unless $ret;

    die "ffmpeg closed: $ret\n${\Dumper $self}" unless $ret > 0;

    # support P6 only
    if (!($header =~ m/^(P6\n(\d+) (\d+)\n(\d+)\n)/)) {
        die "Invalid PPM header: $header";
    }
    my $header_len = length($1);
    my $width = $2;
    my $height = $3;
    my $bytes_per_pixel = ($4 < 256) ? 1 : 2;
    my $frame_len = $width * $height * 3 * $bytes_per_pixel;
    my $remaining_len = $header_len + $frame_len - $ret;
    $ret = $ffmpeg->read(my $frame_data, $remaining_len);
    if ($ret != $remaining_len) {
        bmwqemu::diag "Incomplete frame (got $ret instead of $remaining_len)";
        return undef;
    }

    my $img = tinycv::from_ppm($header . $frame_data);
    $self->{_framebuffer} = $img;
    $self->{width} = $width;
    $self->{height} = $height;
    $self->{_last_update_received} = time;
    return $img;
}

sub _receive_frame_ustreamer ($self) {
    die 'ustreamer is not running. Probably your backend instance could not start or died.'
      unless my $ustreamer = $self->{ustreamer};

    flock($self->{ustreamer}, Fcntl::LOCK_EX);
    my $ustreamer_map;
    map_handle($ustreamer_map, $ustreamer, "+<");
    {
        my $unlock = scope_guard sub {
            unmap($ustreamer_map);
            flock($ustreamer, Fcntl::LOCK_UN);
        };

        # us_memsink_shared_s struct defined in https://github.com/pikvm/ustreamer/blob/master/src/libs/memsinksh.h
        # #define US_MEMSINK_MAGIC    ((uint64_t)0xCAFEBABECAFEBABE)
        # #define US_MEMSINK_VERSION  ((uint32_t)4)
        # typedef struct {
        #     uint64_t    magic;
        #     uint32_t    version;
        #     // pad
        #     uint64_t    id;
        #
        #     size_t      used;
        #     unsigned    width;
        #     unsigned    height;
        #     unsigned    format;
        #     unsigned    stride;
        #     bool        online;
        #     bool        key;
        #     // pad
        #     unsigned    gop;
        #     // 56
        #     long double grab_ts;
        #     long double encode_begin_ts;
        #     long double encode_end_ts;
        #     // 112
        #     long double last_client_ts;
        #     bool        key_requested;
        #
        #     // 129
        #     uint8_t     data[US_MEMSINK_MAX_DATA];
        # } us_memsink_shared_s;
        #
        # #define US_MEMSINK_VERSION  ((u32)7)
        # typedef struct {
        #     uint64_t     magic;
        #     uint32_t     version;
        #     // pad
        #     uint64_t     id;
        #     size_t      used;
        #     // 32
        #     long double     last_client_ts;
        #     bool    key_requested;
        #     // 52
        #     unsigned    width;
        #     unsigned    height;
        #     unsigned    format;
        #     unsigned    stride;
        #     /* Stride is a bytesperline in V4L2 */
        #     /* https://www.kernel.org/doc/html/v4.14/media/uapi/v4l/pixfmt-v4l2.html */
        #     /* https://medium.com/@oleg.shipitko/what-does-stride-mean-in-image-processing-bba158a72bcd */
        #     bool    online;
        #     bool    key;
        #     unsigned    gop;
        #
        #     long double     grab_ts;
        #     long double     encode_begin_ts;
        #     long double     encode_end_ts;
        #     // 128
        #     ... data
        # } us_memsink_shared_s;

        my ($magic, $version, $id, $used) = unpack("QLx4QQ", $ustreamer_map);
        # This is US_MEMSINK_MAGIC, but perl considers hex literals over 32bits non-portable
        if ($magic != 14627333968358193854) {
            bmwqemu::diag "Invalid ustreamer magic: $magic";
            return undef;
        }
        my ($client_clock_offset, $meta_offset, $data_offset);
        if ($version == 4) {
            $client_clock_offset = 112;
            $data_offset = 129;
            $meta_offset = 32;
        } elsif ($version == 7) {
            $client_clock_offset = 32;
            $data_offset = 128;
            $meta_offset = 52;
        } else {
            die "Unsupported ustreamer version '$version' (only versions 4 and 7 are supported)";
        }

        # tell ustreamer we are reading, otherwise it won't write new frames
        my $clock = clock_gettime(CLOCK_MONOTONIC);
        substr($ustreamer_map, $client_clock_offset, 16) = pack("D", $clock);
        # no new frame
        return undef if $self->{ustreamer_last_id} && $id == $self->{ustreamer_last_id};
        $self->{ustreamer_last_id} = $id;
        # empty frame
        return undef unless $used;

        my ($width, $height, $format, $stride) = unpack("IIa4ICCxxI", substr($ustreamer_map, $meta_offset, 28));

        my $img;
        if ($format eq 'JPEG') {
            # tinycv::from_ppm in fact handles a bunch of formats, including JPEG
            $img = tinycv::from_ppm(substr($ustreamer_map, $data_offset, $used));
        } elsif ($format eq 'RGB3') {
            $img = tinycv::new($width, $height);
            my $vncinfo = tinycv::new_vncinfo(
                0,    # do_endian_conversion
                1,    # true_color
                3,    # bytes_per_pixel
                0xff,    # red_mask
                0,    # red_shift
                0xff,    # green_mask
                8,    # green_shift
                0xff,    # blue_mask
                16,    # blue_shift
            );
            $img->map_raw_data(substr($ustreamer_map, $data_offset, $used), 0, 0, $width, $height, $vncinfo);
        } elsif ($format eq 'BGR3') {
            $img = tinycv::new($width, $height);
            my $vncinfo = tinycv::new_vncinfo(
                0,    # do_endian_conversion
                1,    # true_color
                3,    # bytes_per_pixel
                0xff,    # red_mask
                16,    # red_shift
                0xff,    # green_mask
                8,    # green_shift
                0xff,    # blue_mask
                0,    # blue_shift
            );
            $img->map_raw_data(substr($ustreamer_map, $data_offset, $used), 0, 0, $width, $height, $vncinfo);
        } elsif ($format eq 'UYVY') {
            $img = tinycv::new($width, $height);
            $img->map_raw_data_uyvy(substr($ustreamer_map, $data_offset, $used));
        } else {
            die "Unsupported video format '$format'";    # uncoverable statement
        }
        $self->{_framebuffer} = $img;
        $self->{width} = $width;
        $self->{height} = $height;
        $self->{_last_update_received} = time;
        return $img;
    }
}

sub update_framebuffer ($self) {
    if ($self->{dv_timings_supported}) {
        # periodically check if DV timings needs update due to resolution change
        if (time - $self->{dv_timings_last_check} >= DV_TIMINGS_CHECK_INTERVAL) {
            my $current_timings = _v4l2_ctl($self->{args}->{url}, $self->{args}->{video_cmd_prefix}, '--query-dv-timings');
            if ($current_timings && $current_timings ne $self->{dv_timings}) {
                bmwqemu::diag "Updating DV timings, new: $current_timings";
                # yes, there is need to update DV timings, restart ffmpeg,
                # connect_remote_video will update the timings
                $self->disable_video;
                $self->connect_remote_video($self->{args}->{url});
            } elsif ($self->{dv_timings} && !$current_timings) {
                bmwqemu::diag "video disconnected";
                $self->disable_video;
                $self->{dv_timings} = '';
            }
            $self->{dv_timings_last_check} = time;
        }
    }

    # no video connected, don't read anything
    return 0 unless $self->{ffmpeg} or $self->{ustreamer};

    my $have_received_update = 0;
    if ($self->{ffmpeg}) {
        while ($self->_receive_frame_ffmpeg()) {
            $have_received_update = 1;
        }
    } elsif ($self->{ustreamer}) {
        # shared-memory interface "discards" older frames implicitly,
        # no need to loop
        if ($self->_receive_frame_ustreamer()) {
            $have_received_update = 1;
        }
    }
    return $have_received_update;
}

sub current_screen ($self) {
    $self->update_framebuffer();
    return unless $self->{_framebuffer};
    return $self->{_framebuffer};
}

sub request_screen_update ($self, @) {
    if (!$self->update_framebuffer()) {
        # check if it isn't stalled, perhaps we missed resolution change?
        my $time_since_last_update = time - $self->{_last_update_received};
        if ($self->{ffmpeg} && $time_since_last_update > STALL_THRESHOLD) {
            # reconnect, it will refresh the device settings too
            $self->disable_video;
            $self->connect_remote_video($self->{args}->{url});
        }
    }
}

sub send_key_event ($self, $key, $press_release_delay) {
    return unless $self->{input_pipe};
    $self->{input_pipe}->write($key . "\n")
      or die "failed to send '$key' input event";
    my $rsp = $self->{input_feedback}->getline;
    die "Send key failed: $rsp" unless $rsp eq "ok\n";
}

=head2 _send_keyboard_emulator_cmd

	_send_keyboard_emulator_cmd($self, %args)

Send keyboard events using RPi Pico W based keyboard emulator

Args to be used:

	type => "hallo welt\n"
	sendkey => "ctrl-alt-del"

Intended to be used together with this device:
https://github.com/os-autoinst/os-autoinst-distri-opensuse/tree/master/data/generalhw_scripts/rpi_pico_w_keyboard

=cut

sub _send_keyboard_emulator_cmd ($self, %args) {
    my $keyboard_device_url = $bmwqemu::vars{GENERAL_HW_KEYBOARD_URL};
    my $url = Mojo::URL->new($keyboard_device_url)->query(%args);
    $self->{_ua} //= Mojo::UserAgent->new;
    my $server_response = $self->{_ua}->get($url)->result->body;
    chomp($server_response);
    bmwqemu::diag("Keyboard emulator says: " . bmwqemu::pp($server_response));
    return {};
}


sub type_string ($self, $args) {
    if ($bmwqemu::vars{GENERAL_HW_KEYBOARD_URL}) {
        return $self->_send_keyboard_emulator_cmd(type => $args->{text});
    }
    return $self->SUPER::type_string($args);
}

sub send_key ($self, $args) {
    if ($bmwqemu::vars{GENERAL_HW_KEYBOARD_URL}) {
        return $self->_send_keyboard_emulator_cmd(sendkey => $args->{key});
    }
    return $self->SUPER::send_key($args);
}

sub get_last_mouse_set ($self, @) {
    return $self->{mouse};
}

sub mouse_move_to ($self, $x, $y) {
    return unless $self->{input_pipe};
    $self->{input_pipe}->write("mouse_move $x $y\n");
    $self->{input_pipe}->flush;
    my $rsp = $self->{input_feedback}->getline;
    die "Mouse move failed: $rsp" unless $rsp eq "ok\n";
}

sub mouse_button ($self, $args) {
    return unless $self->{input_pipe};
    my $button = $args->{button};
    my $bstate = $args->{bstate};
    # careful: the bits order is different than in VNC
    my $mask = {left => $bstate, right => $bstate << 1, middle => $bstate << 2}->{$button} // 0;
    bmwqemu::diag "pointer_event $mask $self->{mouse}->{x}, $self->{mouse}->{y}";
    $self->{input_pipe}->write("mouse_button $mask\n");
    $self->{input_pipe}->flush;
    my $rsp = $self->{input_feedback}->getline;
    die "Mouse button failed: $rsp" unless $rsp eq "ok\n";
    return {};
}

1;
