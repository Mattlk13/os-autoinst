use base "installstep";
use strict;
use bmwqemu;

sub run()
{
	sleep 30;
#	waitidle();
	waitstillimage(10,100);
	mouse_hide();
	sleep 1;
	waitgoodimage(10);
#	avgcolor=0.684,0.714,0.733
	sendkey "end";
	sendkey "ret";
	sendkey "down";
	sendkey "down";
	sendkey "tab"; # skip media check
	sendkey "tab"; # skip media check
	sendkey "tab"; # skip media check
	sendkey "ret";
}

1;