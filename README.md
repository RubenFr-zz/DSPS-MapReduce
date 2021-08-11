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

In the first round we want to compute N, c(w1) and c(w1,w2)

### Map:
__input:__ "w1_w2 year occurrences ?? ??"  
__output 1:__ K = decade w1 w2, V = occurrences  
__output 2:__ K = decade w1 *, V = occurrences  
__output 3:__ K = decade * w2, V = occurrences  

### Shuffle and Sort:
Distribute the K-V formed according to decades they're in  
Sort them according to w1 and then w2 (gram containing * are before everything)

### Reduce:
__input 1:__ K = decade w1 *,  V = [occ1, occ2, ...]  
__input 2:__ K = decade * w2,  V = [occ1, occ2, ...]  
__input 3:__ K = decade w1 w2,  V = [occ1, occ2, ...]  
__output:__ `decade w1 w2 TAB c_w1_w2 c_w1 (or c_w2) N`

> At the end of this Round we have 10 files (5 for c_w1 and 5 for c_w2).

```text
1670 אבא בעיר	4 145540 522190510
1700 אבא עוד	1 145540 522190510
1750 אבא אבוה	2 145540 522190510
1750 אבא באהל	1 145540 522190510
1760 אבא ושלא	1 145540 522190510
1790 אבא אבוה	1 145540 522190510
1790 אבא וכו	1 145540 522190510
1790 אבא כתב	1 145540 522190510
1790 אבא מרי	3 145540 522190510
1790 אבא שמואל	1 145540 522190510
1800 אבא לבאר	1 145540 522190510
1800 אבא לביתך	1 145540 522190510
```

```text
Map-Reduce Framework
		Map input records=252069581
		Map output records=464423694
		Map output bytes=13123551456
		Map output materialized bytes=471683939
		Input split bytes=5304
		Combine input records=518959514
		Combine output records=108794567
		Reduce input groups=52795887
		Reduce shuffle bytes=471683939
		Reduce input records=54258747
		Reduce output records=52535564
		Spilled Records=163593534
		Shuffled Maps =390
		Failed Shuffles=0
		Merged Map outputs=390
		GC time elapsed (ms)=284624
		CPU time spent (ms)=17763560
		Physical memory (bytes) snapshot=47255109632
		Virtual memory (bytes) snapshot=166547787776
		Total committed heap usage (bytes)=42389733376
		Peak Map Physical memory (bytes)=891564032
		Peak Map Virtual memory (bytes)=3167502336
		Peak Reduce Physical memory (bytes)=1353326592
		Peak Reduce Virtual memory (bytes)=4540825600
LocalApplication.Constants$COUNTERS
		COUNTED=116105826
		NOT_COUNTED=135963755
```

## Round 2

Regroupment of all parameters and Calculation of nmpi  

```text
npmi(w1,w2) = pmi(w1,w2) / -log[p(w1,w2)]
pmi(w1,w2) = log[c(w1,w2)] + log(N) - log[c(w1)] - log[c(w2)]
```

### Map:
__input:__ "decade w1 w2 TAB c_w1_w2 c_w1 (or c_w2) N"  
__output:__ K = Gram2 Object, V = c_w1_w2 c_w1 (or c_w2) N  

> The mapper doesn't do anything but restructure the input 

### Shuffle and Sort:
According to if we want to calculate c_w1 or c_w2 we sort the mapper outputs
based on w1 or w2 (* are always prioritized)  
If same w1 or w2 then sort by decade

### Reduce:
__input:__ K = Gram2 Object,  V = [c_w1_w2 c_w1 N, c_w1_w2 c_w2 N]  
__output:__ `decade w1 w2 TAB npmi`

> At the end of this Round we have 5 files regrouping all the corpus npmi's

```text
Map-Reduce Framework
		Map input records=52535564
		Map output records=52535564
		Map output bytes=2242112215
		Map output materialized bytes=802475985
		Input split bytes=9560
		Combine input records=0
		Combine output records=0
		Reduce input groups=26267782
		Reduce shuffle bytes=802475985
		Reduce input records=52535564
		Reduce output records=26267782
		Spilled Records=105071128
		Shuffled Maps =200
		Failed Shuffles=0
		Merged Map outputs=200
		GC time elapsed (ms)=49624
		CPU time spent (ms)=1910560
		Physical memory (bytes) snapshot=43059515392
		Virtual memory (bytes) snapshot=147474329600
		Total committed heap usage (bytes)=41073770496
		Peak Map Physical memory (bytes)=1070473216
		Peak Map Virtual memory (bytes)=3177353216
		Peak Reduce Physical memory (bytes)=1837010944
		Peak Reduce Virtual memory (bytes)=4466647040
LocalApplication.Constants$COUNTERS
		NEGATIVE=10570157
		POSITIVE=15697625
```

## Round 3

Compute the relative npmi and filter out not high enough values

```text
rel_npmi(w1,w2) = npmi(w1,w2) / relativePmi(dec)  
relativePmi(dec) is the sum of all normalized pmi in the same decade (including those which their normalized PMI is less than minPmi)
```

```text
Filter out if: npmi < minPmi OR rel_npmi < relMinPmi
```

### Map:
__input:__ "decade w1 w2 TAB npmi"  
__output 1:__ K = decade w1 w2, V = npmi  
__output 2:__ K = decade *  *, V = npmi

### Shuffle and Sort:
No need to shuffle -> only 1 reducer
Sort according to Decade then to npmi then W1 and finally W2

### Reduce:
__input 1:__ K = decade *  *, V = npmi  
__input 2:__ K = decade w1 w2, V = [npmi_1, npmi_2, ...]  
__output:__ `decade TAB TAB npmi = npmi(w1,w2) TAB TAB w1 w2`

```text
1530-1539		nmpi = 0.59042		מפתיה והולכתיה	
1530-1539		nmpi = 0.51307		בפוך אבניך	
1540-1549		nmpi = 0.61224		גמלתי שולמי	
1540-1549		nmpi = 0.61023		הכמתא להכימין	
1540-1549		nmpi = 0.60632		האודם והלובן	
1540-1549		nmpi = 0.60073		מהשבותי מהשבותיכם	
1540-1549		nmpi = 0.59042		שרותיה תענינה	
1540-1549		nmpi = 0.5888		וארפאהו ואנחהו	
1540-1549		nmpi = 0.56647		מזכיותיו מטיבין	
1540-1549		nmpi = 0.56066		ההנאות הגופיות	
1540-1549		nmpi = 0.5562		וירעיבך ויאכילך	
1540-1549		nmpi = 0.55052		עוזך וכבודך	
1540-1549		nmpi = 0.54999		מועדיה ותור	
```

```text
Map-Reduce Framework
		Map input records=26267782
		Map output records=52535564
		Map output bytes=1686599606
		Map output materialized bytes=662789680
		Input split bytes=4780
		Combine input records=52535564
		Combine output records=26267830
		Reduce input groups=26267815
		Reduce shuffle bytes=662789680
		Reduce input records=26267830
		Reduce output records=420576
		Spilled Records=52535660
		Shuffled Maps =20
		Failed Shuffles=0
		Merged Map outputs=20
		GC time elapsed (ms)=24787
		CPU time spent (ms)=1186810
		Physical memory (bytes) snapshot=19906605056
		Virtual memory (bytes) snapshot=67160264704
		Total committed heap usage (bytes)=18656264192
		Peak Map Physical memory (bytes)=1101332480
		Peak Map Virtual memory (bytes)=3162398720
		Peak Reduce Physical memory (bytes)=2311213056
		Peak Reduce Virtual memory (bytes)=4462874624
LocalApplication.Constants$COUNTERS
		FILTERED=25847206
		NOT_FILTERED=420576
```    
