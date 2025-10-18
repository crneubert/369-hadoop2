Carson Neubert -- CS369 -- Lab 3

1)

CountryRequestCount.java has two mappers. The first map intakes hostname_countries.csv and outputs <hostname, country>.
The second map intakes access.log and outputs <hostname, 1>. The reducer performs the join, intaking outputs from both
mappers and outputting <country, count>. I chose the reduce-side join here as it made more sense to me conceptually and
can handle larger files better than a map-side join.

SumCountry.java intakes the previous output and maps <country, count> and then reduces to <country, sum of counts> so
there is only one observation per country.

AccessLog2.java is the given code from Lab 2 which intakes the previous output and returns the same <country, sum> pairs
but sorted in descending order. The descending order was obtained using a decreasing comparator in the Hadoop drivers.


2)

URLCountryCount.java has two mappers. The first map intakes hostname_countries.csv and outputs <hostname, country>.
The second map intakes access.log and outputs <hostname, URL>. The reducer performs the join, intaking outputs from both
mappers and outputting <country URL, one>.

The following output is then fed into SumCountry.java which sums up each country URL pair and outputs
<country URL, count>

URLCountrySorter.java intakes the previous output and has a mapper which returns <(country, count), url count>.
Country and Count are stored as a CountryCountPair class which has a compareTo function which ensures proper ordering
of countries first alphabetically and then counts to break ties. The key, values are then sorted accordingly and output
by the reducer as <country, URL count>


3)

The same output from #2 post SumCountry.java <country URL, count> is now fed into URLListCountry.java which has one
mapper which outputs <url, country> and is then reduced by the reducer into <url, list of all unique countries>.