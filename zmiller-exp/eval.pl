#!/usr/bin/perl

open EXACT, "<exact.result";
while ($l = <EXACT>) {
  $l =~ /(.*)\ =\ ([0-9]+)/;
  $k = $1;
  $c = $2;
  $EXACT{$k} = $c;
  $TOTAL += $c;
}

$COUNT = 0;
$DIFFERS = 0;
$ERROR = 0;

while ($l = <>) {
  $l =~ /(.*)\ =\ ([0-9]+)/;
  $k = $1;
  $c = $2;
  
  $COUNT++;
  if($EXACT{$k} != $c) {
    $DIFFERS++;
    $ERROR += ($c - $EXACT{$k});
  }
}

print "of $COUNT groups ($TOTAL records):\n";
printf("%s were overestimates (%0.2f)%%\n", $DIFFERS, 100.0 * ($DIFFERS / $COUNT));
printf("total error was %i (%0.2f)%%\n", $ERROR, 100.0 * ($ERROR/$TOTAL));
printf("average error was %f\n", ($ERROR / $COUNT));

