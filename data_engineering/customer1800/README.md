This code solves 1-800-CODING-CHALLENGE problem. 

This code works by pre-processing the dictionary words to get the corresponding digit string. Once we get the different number combinations, this problem is reduced word-break problem where the words are actually smaller permutations of numbers coming from dictionary and the parent string is the given phone number. 
Once the word break method executes, the result is mapped back to the original dictionary words. 

build by
```
mvn package
```

run by

```
java -jar target/customer1800-0.0.1-SNAPSHOT.jar -d src/main/resources/dict.txt
```

or 

```
java -jar target/customer1800-0.0.1-SNAPSHOT.jar -d src/main/resources/dict.txt src/main/resources/numbers.txt
```

This problem was chosen since it had an interesting underlying algorithm to work out its logic. I first proceeded with finding all words from a phone number and then finding valid words from dictionary. This was a costly operation (word break itself O(2^n), different permutations of words from phone number , total 1296 permutations, means 1296 times of checking for word break for each given phone number). Later, it struck me that dictionary can be pre-processed. Even in the real world, something like a dictionary is not likely to change a lot. After this pre-processing, word break is done only once for each given phone number. So complexity is O(k*m+O(2^n)), where k is the numbers of words in dictionary, m is the length of each word, n is the length of the phone number.

