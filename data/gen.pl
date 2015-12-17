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

  if ($p < 2) {
    print "A\n";
  } else {
    if ($p < 4) {
      print "B\n";
    } else {
      if ($p < 6) {
        print "C\n";
      } else {
        if ($p < 7) {
          print "D\n";
        } else {
          if ($p < 8) {
            print "E\n";
          } else {
            $p = rand(21);
            print (chr(70 + $p));
            print "\n";
          }
        }
      }
    }
  }
}

