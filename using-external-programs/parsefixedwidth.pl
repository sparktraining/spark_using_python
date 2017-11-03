#!/usr/bin/env perl
my $format = 'A6 A8 A8 A12 A2 A5';
while (<>) {
	chomp;
	my( $custid, $orderid, $date,
	 $city, $state, $zip) =
	unpack( $format, $_ );
	print "$custid\t$orderid\t$date\t$city\t$state\t$zip";
}
