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

  for($j = 0; $j < 5; $j++) {

    $p = rand(10);

    if ($p < 3) {
      print "A";
    } else {
      if ($p < 6) {
        print "B";
      } else {
        if ($p < 9) {
          print "C";
        } else {
          $p = rand(23);
          print (chr(68 + $p));
        }
      }
    }
  }

  print "\n";
}

