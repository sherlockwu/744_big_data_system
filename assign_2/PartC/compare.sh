echo $1 $2

comm -12 <( sort $1 ) <( sort $2 ) > $3
