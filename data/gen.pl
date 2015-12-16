#!/usr/bin/perl

$rn = 0;

$NUM_ENTRIES = $ARGV[0];

if ($NUM_ENTRIES == 0) {
  $NUM_ENTRIES=255;
}

for ($i = 0; $i < $NUM_ENTRIES; $i++) {

  ranval($i);

}


sub ranval {

  $p = rand(10);

  if ($p < 8) {
    print "A\n";
  }
  if ($p < 7) {
    print "B\n";
  }
  if ($p < 5) {
    print "C\n";
  }
  if ($p < 3) {
    print "D\n";
  }
  if ($p < 1) {
    print "E\n";
  }

  $p = rand(21);
  print (chr(66 + $p));

  print "\n";

}

