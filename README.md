# DSPS Task 2 - Map Reduce

Google 2-gram

<p align="center">
  <a href="#dsps-task-2---map-reduce"><img src="https://miro.medium.com/max/4000/1*b_al7C5p26tbZG4sy-CWqw.png" width="350" title="AWS" target="_blank"/></a>
</p>

## Map Reduce model

The input is from the **[Google 2-gram](http://storage.googleapis.com/books/ngrams/books/datasetsv2.html)** dataset: `ngram TAB year TAB match_count TAB volume_count NEWLINE`  
Good ref: [Click Here](https://github.com/MaorRocky/Collocation-Extraction-Amazon-EMR)

N           = total number of bi-grams in the corpus (number of rows)  
c(w1, w2)   = count of the bigram w1w2 in a decade  
c(w1)       = count of bigrams that starts with w1 in the entire corpus  
c(w2)       = count of bigrams that ends with w2 in the entire corpus  

## Round 1

In the first round we want to compute N, c(w1), c(w2) and 

Map:
input: "w1_w2 year occurrences ?? ??"
output: {K=year, V= {w1_w2, occurences}}

Reduce:
input: List[{Y1, [w11,w12]},{Y2, [w21,w22]},...,{Yn,[wn1,wn2]}]
output: List[{K=Y1, V=List[[w11,w12],[w21,w22],...,[wn1,wn2]},...,{K=Yn, V=List[[w11,w12],[w21,w22],...,[wn1,wn2]}}]

1990 {w11_w12, occ1} {w21_w22, occ2} ... {wn1,wn2, occn}
2000 ...

## Round 2

Count the number of times w1 and w2 appears each decade

Map
input: [w1,w2]
output: {w1,1} {w2,1}

Reduce
input: List[{w1,1},{w1,1}...]
ouput: {K=w1, V=10}

## Round 3

Compute the npmi

```LaTex
npmi(w1,w2)=\frac{pmi(w1,w2)}{-log[p(w1,w2)]}
pmi(w1,w2)=log[c(w1,w2)]+log(N)-log(c(w1))-log(c(w2))
```

Map:
input: w1_w2 x w1 y w2 z N
output: w1_w2 N x y z

Reduce:
input: 1_w2 N x y z
output:	w1_w2 npmi(w1,w2)

## Round 4

Compare the npmi's obtained in Round 3 and add them to the final list if 
```LaTex
npmi < minPmi AND npmi < relMinPmi
```

Map:
input: w1_w2 npmi
