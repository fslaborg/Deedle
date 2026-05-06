# Deedle in 10 minutes

This quick-start gets you productive with Deedle fast.
We use the classic [Titanic passenger data set](http://www.kaggle.com/c/titanic-gettingStarted)
to show the most important features — loading data, exploring columns,
filtering, grouping, handling missing values, and basic statistics.

## Setup

Install from NuGet and open the namespace:

dotnet add package Deedle

```fsharp
#r "nuget: Deedle"
open Deedle
```

## Loading data

The fastest way to get data into Deedle is `Frame.ReadCsv`:

```fsharp
let titanic = Frame.ReadCsv(root + "titanic.csv")
```

Let's see what we have:

```fsharp
titanic.RowCount
```

```
val it: int = 891
```

```fsharp
titanic.ColumnCount
```

```
val it: int = 12
```

```fsharp
titanic.ColumnKeys |> Seq.toList
```

```
val it: string list =
  ["PassengerId"; "Survived"; "Pclass"; "Name"; "Sex"; "Age"; "SibSp"; "Parch";
   "Ticket"; "Fare"; "Cabin"; "Embarked"]
```

Print the first few rows:

```fsharp
titanic |> Frame.take 5
```

```
val it: Frame<int,string> =
  
     PassengerId Survived Pclass Name                                               Sex    Age SibSp Parch Ticket           Fare    Cabin Embarked 
0 -> 1           False    3      Braund, Mr. Owen Harris                            male   22  1     0     A/5 21171        7.25          S        
1 -> 2           True     1      Cumings, Mrs. John Bradley (Florence Briggs Tha... female 38  1     0     PC 17599         71.2833 C85   C        
2 -> 3           True     3      Heikkinen, Miss. Laina                             female 26  0     0     STON/O2. 3101282 7.925         S        
3 -> 4           True     1      Futrelle, Mrs. Jacques Heath (Lily May Peel)       female 35  1     0     113803           53.1    C123  S        
4 -> 5           False    3      Allen, Mr. William Henry                           male   35  0     0     373450           8.05          S
```

## Accessing columns

Use `?` to grab a column as a `Series`. Deedle reads numeric columns as `float`
and text columns as `string`:

```fsharp
// A numeric column
titanic?Age
```

```
val it: Series<int,float> =
  
0   -> 22        
1   -> 38        
2   -> 26        
3   -> 35        
4   -> 35        
5   -> <missing> 
6   -> 54        
7   -> 2         
8   -> 27        
9   -> 14        
10  -> 4         
11  -> 58        
12  -> 20        
13  -> 39        
14  -> 14        
... -> ...       
876 -> 20        
877 -> 19        
878 -> <missing> 
879 -> 56        
880 -> 25        
881 -> 33        
882 -> 22        
883 -> 28        
884 -> 25        
885 -> 39        
886 -> 27        
887 -> 19        
888 -> <missing> 
889 -> 26        
890 -> 32
```

```fsharp
// A text column
titanic.GetColumn<string>("Name") |> Series.take 3
```

```
val it: Series<int,string> =
  
0 -> Braund, Mr. Owen Harris                            
1 -> Cumings, Mrs. John Bradley (Florence Briggs Tha... 
2 -> Heikkinen, Miss. Laina
```

## Basic statistics

Compute summary statistics on any numeric series:

```fsharp
titanic?Age |> Stats.mean
```

```
val it: float = 29.69911765
```

```fsharp
titanic?Fare |> Stats.mean
```

```
val it: float = 32.20420797
```

```fsharp
titanic?Age |> Stats.median
```

```
val it: float = 28.0
```

```fsharp
titanic?Age |> Stats.stdDev
```

```
val it: float = 14.52649733
```

Missing values (like missing `Age` entries in the Titanic data) are automatically
skipped by all statistical functions.

## Filtering rows

Use `Frame.filterRowValues` to keep only rows matching a condition:

```fsharp
// Passengers who survived
let survived = titanic |> Frame.filterRowValues (fun row ->
  row.GetAs<bool>("Survived"))

survived.RowCount
```

```
val survived: Frame<int,string> =
  
       PassengerId Survived Pclass Name                                               Sex    Age       SibSp Parch Ticket           Fare     Cabin Embarked 
1   -> 2           True     1      Cumings, Mrs. John Bradley (Florence Briggs Tha... female 38        1     0     PC 17599         71.2833  C85   C        
2   -> 3           True     3      Heikkinen, Miss. Laina                             female 26        0     0     STON/O2. 3101282 7.925          S        
3   -> 4           True     1      Futrelle, Mrs. Jacques Heath (Lily May Peel)       female 35        1     0     113803           53.1     C123  S        
8   -> 9           True     3      Johnson, Mrs. Oscar W (Elisabeth Vilhelmina Berg)  female 27        0     2     347742           11.1333        S        
9   -> 10          True     2      Nasser, Mrs. Nicholas (Adele Achem)                female 14        1     0     237736           30.0708        C        
10  -> 11          True     3      Sandstrom, Miss. Marguerite Rut                    female 4         1     1     PP 9549          16.7     G6    S        
11  -> 12          True     1      Bonnell, Miss. Elizabeth                           female 58        0     0     113783           26.55    C103  S        
15  -> 16          True     2      Hewlett, Mrs. (Mary D Kingcome)                    female 55        0     0     248706           16             S        
17  -> 18          True     2      Williams, Mr. Charles Eugene                       male   <missing> 0     0     244373           13             S        
19  -> 20          True     3      Masselmani, Mrs. Fatima                            female <missing> 0     0     2649             7.225          C        
21  -> 22          True     2      Beesley, Mr. Lawrence                              male   34        0     0     248698           13       D56   S        
22  -> 23          True     3      McGowan, Miss. Anna "Annie"                        female 15        0     0     330923           8.0292         Q        
23  -> 24          True     1      Sloper, Mr. William Thompson                       male   28        0     0     113788           35.5     A6    S        
25  -> 26          True     3      Asplund, Mrs. Carl Oscar (Selma Augusta Emilia ... female 38        1     5     347077           31.3875        S        
28  -> 29          True     3      O'Dwyer, Miss. Ellen "Nellie"                      female <missing> 0     0     330959           7.8792         Q        
:      ...         ...      ...    ...                                                ...    ...       ...   ...   ...              ...      ...   ...      
855 -> 856         True     3      Aks, Mrs. Sam (Leah Rosen)                         female 18        0     1     392091           9.35           S        
856 -> 857         True     1      Wick, Mrs. George Dennick (Mary Hitchcock)         female 45        1     1     36928            164.8667       S        
857 -> 858         True     1      Daly, Mr. Peter Denis                              male   51        0     0     113055           26.55    E17   S        
858 -> 859         True     3      Baclini, Mrs. Solomon (Latifa Qurban)              female 24        0     3     2666             19.2583        C        
862 -> 863         True     1      Swift, Mrs. Frederick Joel (Margaret Welles Bar... female 48        0     0     17466            25.9292  D17   S        
865 -> 866         True     2      Bystrom, Mrs. (Karolina)                           female 42        0     0     236852           13             S        
866 -> 867         True     2      Duran y More, Miss. Asuncion                       female 27        1     0     SC/PARIS 2149    13.8583        C        
869 -> 870         True     3      Johnson, Master. Harold Theodor                    male   4         1     1     347742           11.1333        S        
871 -> 872         True     1      Beckwith, Mrs. Richard Leonard (Sallie Monypeny)   female 47        1     1     11751            52.5542  D35   S        
874 -> 875         True     2      Abelson, Mrs. Samuel (Hannah Wizosky)              female 28        1     0     P/PP 3381        24             C        
875 -> 876         True     3      Najib, Miss. Adele Kiamie "Jane"                   female 15        0     0     2667             7.225          C        
879 -> 880         True     1      Potter, Mrs. Thomas Jr (Lily Alexenia Wilson)      female 56        0     1     11767            83.1583  C50   C        
880 -> 881         True     2      Shelley, Mrs. William (Imanita Parrish Hall)       female 25        0     1     230433           26             S        
887 -> 888         True     1      Graham, Miss. Margaret Edith                       female 19        0     0     112053           30       B42   S        
889 -> 890         True     1      Behr, Mr. Karl Howell                              male   26        0     0     111369           30       C148  C        

val it: int = 342
```

```fsharp
// First-class passengers
let firstClass = titanic |> Frame.filterRowValues (fun row ->
  row.GetAs<int>("Pclass") = 1)

firstClass.RowCount
```

```
val firstClass: Frame<int,string> =
  
       PassengerId Survived Pclass Name                                               Sex    Age       SibSp Parch Ticket   Fare     Cabin       Embarked 
1   -> 2           True     1      Cumings, Mrs. John Bradley (Florence Briggs Tha... female 38        1     0     PC 17599 71.2833  C85         C        
3   -> 4           True     1      Futrelle, Mrs. Jacques Heath (Lily May Peel)       female 35        1     0     113803   53.1     C123        S        
6   -> 7           False    1      McCarthy, Mr. Timothy J                            male   54        0     0     17463    51.8625  E46         S        
11  -> 12          True     1      Bonnell, Miss. Elizabeth                           female 58        0     0     113783   26.55    C103        S        
23  -> 24          True     1      Sloper, Mr. William Thompson                       male   28        0     0     113788   35.5     A6          S        
27  -> 28          False    1      Fortune, Mr. Charles Alexander                     male   19        3     2     19950    263      C23 C25 C27 S        
30  -> 31          False    1      Uruchurtu, Don. Manuel E                           male   40        0     0     PC 17601 27.7208              C        
31  -> 32          True     1      Spencer, Mrs. William Augustus (Marie Eugenie)     female <missing> 1     0     PC 17569 146.5208 B78         C        
34  -> 35          False    1      Meyer, Mr. Edgar Joseph                            male   28        1     0     PC 17604 82.1708              C        
35  -> 36          False    1      Holverson, Mr. Alexander Oskar                     male   42        1     0     113789   52                   S        
52  -> 53          True     1      Harper, Mrs. Henry Sleeper (Myna Haxtun)           female 49        1     0     PC 17572 76.7292  D33         C        
54  -> 55          False    1      Ostby, Mr. Engelhart Cornelius                     male   65        0     1     113509   61.9792  B30         C        
55  -> 56          True     1      Woolner, Mr. Hugh                                  male   <missing> 0     0     19947    35.5     C52         S        
61  -> 62          True     1      Icard, Miss. Amelie                                female 38        0     0     113572   80       B28                  
62  -> 63          False    1      Harris, Mr. Henry Birkhardt                        male   45        1     0     36973    83.475   C83         S        
:      ...         ...      ...    ...                                                ...    ...       ...   ...   ...      ...      ...         ...      
829 -> 830         True     1      Stone, Mrs. George Nelson (Martha Evelyn)          female 62        0     0     113572   80       B28                  
835 -> 836         True     1      Compton, Miss. Sara Rebecca                        female 39        1     1     PC 17756 83.1583  E49         C        
839 -> 840         True     1      Marechal, Mr. Pierre                               male   <missing> 0     0     11774    29.7     C47         C        
842 -> 843         True     1      Serepeca, Miss. Augusta                            female 30        0     0     113798   31                   C        
849 -> 850         True     1      Goldenberg, Mrs. Samuel L (Edwiga Grabowska)       female <missing> 1     0     17453    89.1042  C92         C        
853 -> 854         True     1      Lines, Miss. Mary Conover                          female 16        0     1     PC 17592 39.4     D28         S        
856 -> 857         True     1      Wick, Mrs. George Dennick (Mary Hitchcock)         female 45        1     1     36928    164.8667             S        
857 -> 858         True     1      Daly, Mr. Peter Denis                              male   51        0     0     113055   26.55    E17         S        
862 -> 863         True     1      Swift, Mrs. Frederick Joel (Margaret Welles Bar... female 48        0     0     17466    25.9292  D17         S        
867 -> 868         False    1      Roebling, Mr. Washington Augustus II               male   31        0     0     PC 17590 50.4958  A24         S        
871 -> 872         True     1      Beckwith, Mrs. Richard Leonard (Sallie Monypeny)   female 47        1     1     11751    52.5542  D35         S        
872 -> 873         False    1      Carlsson, Mr. Frans Olof                           male   33        0     0     695      5        B51 B53 B55 S        
879 -> 880         True     1      Potter, Mrs. Thomas Jr (Lily Alexenia Wilson)      female 56        0     1     11767    83.1583  C50         C        
887 -> 888         True     1      Graham, Miss. Margaret Edith                       female 19        0     0     112053   30       B42         S        
889 -> 890         True     1      Behr, Mr. Karl Howell                              male   26        0     0     111369   30       C148        C        

val it: int = 216
```

## Adding computed columns

The `?<-` operator adds or replaces a column. Let's add a `HasCabin` flag:

```fsharp
titanic?HasCabin <- titanic.GetColumn<string>("Cabin")
  |> Series.mapAll (fun _ v -> Some(v.IsSome))

titanic.Columns.[ ["Name"; "Pclass"; "HasCabin"] ] |> Frame.take 5
```

```
val it: Frame<int,string> =
  
     Name                                               Pclass HasCabin 
0 -> Braund, Mr. Owen Harris                            3      True     
1 -> Cumings, Mrs. John Bradley (Florence Briggs Tha... 1      True     
2 -> Heikkinen, Miss. Laina                             3      True     
3 -> Futrelle, Mrs. Jacques Heath (Lily May Peel)       1      True     
4 -> Allen, Mr. William Henry                           3      True
```

## Grouping and aggregation

Group rows by a column and aggregate — one of Deedle's most powerful features.

### Survival rate by passenger class

```fsharp
titanic
|> Frame.aggregateRowsBy ["Pclass"] ["Fare"] Stats.mean
```

```
val it: Frame<int,string> =
  
     Pclass Fare               
0 -> 3      13.675550101832997 
1 -> 1      84.15468749999992  
2 -> 2      20.66218315217391
```

### Survival counts by class and sex

```fsharp
let byClassAndSex = 
  titanic
  |> Frame.groupRowsByInt "Pclass"
  |> Frame.groupRowsByString "Sex"
  |> Frame.mapRowKeys Pair.flatten3

byClassAndSex.GetColumn<bool>("Survived")
|> Series.applyLevel Pair.get1And2Of3 (fun s ->
    series (Seq.countBy id s.Values))
|> Frame.ofRows
```

```
val byClassAndSex: Frame<(string * int * int),string> =
  
                PassengerId Survived Pclass Name                                          Sex    Age       SibSp Parch Ticket          Fare    Cabin Embarked HasCabin 
male   3 0   -> 1           False    3      Braund, Mr. Owen Harris                       male   22        1     0     A/5 21171       7.25          S        True     
male   3 4   -> 5           False    3      Allen, Mr. William Henry                      male   35        0     0     373450          8.05          S        True     
male   3 5   -> 6           False    3      Moran, Mr. James                              male   <missing> 0     0     330877          8.4583        Q        True     
male   3 7   -> 8           False    3      Palsson, Master. Gosta Leonard                male   2         3     1     349909          21.075        S        True     
male   3 12  -> 13          False    3      Saundercock, Mr. William Henry                male   20        0     0     A/5. 2151       8.05          S        True     
male   3 13  -> 14          False    3      Andersson, Mr. Anders Johan                   male   39        1     5     347082          31.275        S        True     
male   3 16  -> 17          False    3      Rice, Master. Eugene                          male   2         4     1     382652          29.125        Q        True     
male   3 26  -> 27          False    3      Emir, Mr. Farred Chehab                       male   <missing> 0     0     2631            7.225         C        True     
male   3 29  -> 30          False    3      Todoroff, Mr. Lalio                           male   <missing> 0     0     349216          7.8958        S        True     
male   3 36  -> 37          True     3      Mamee, Mr. Hanna                              male   <missing> 0     0     2677            7.2292        C        True     
male   3 37  -> 38          False    3      Cann, Mr. Ernest Charles                      male   21        0     0     A./5. 2152      8.05          S        True     
male   3 42  -> 43          False    3      Kraeff, Mr. Theodor                           male   <missing> 0     0     349253          7.8958        C        True     
male   3 45  -> 46          False    3      Rogers, Mr. William John                      male   <missing> 0     0     S.C./A.4. 23567 8.05          S        True     
male   3 46  -> 47          False    3      Lennon, Mr. Denis                             male   <missing> 1     0     370371          15.5          Q        True     
male   3 48  -> 49          False    3      Samaan, Mr. Youssef                           male   <missing> 2     0     2662            21.6792       C        True     
:      : :      ...         ...      ...    ...                                           ...    ...       ...   ...   ...             ...     ...   ...      ...      
female 2 706 -> 707         True     2      Kelly, Mrs. Florence "Fannie"                 female 45        0     0     223596          13.5          S        True     
female 2 717 -> 718         True     2      Troutt, Miss. Edwina Celia "Winnie"           female 27        0     0     34218           10.5    E101  S        True     
female 2 720 -> 721         True     2      Harper, Miss. Annie Jessie "Nina"             female 6         0     1     248727          33            S        True     
female 2 726 -> 727         True     2      Renouf, Mrs. Peter Henry (Lillian Jefferys)   female 30        3     0     31027           21            S        True     
female 2 747 -> 748         True     2      Sinkkonen, Miss. Anna                         female 30        0     0     250648          13            S        True     
female 2 750 -> 751         True     2      Wells, Miss. Joan                             female 4         1     1     29103           23            S        True     
female 2 754 -> 755         True     2      Herman, Mrs. Samuel (Jane Laver)              female 48        1     2     220845          65            S        True     
female 2 772 -> 773         False    2      Mack, Mrs. (Mary)                             female 57        0     0     S.O./P.P. 3     10.5    E77   S        True     
female 2 774 -> 775         True     2      Hocking, Mrs. Elizabeth (Eliza Needs)         female 54        1     3     29105           23            S        True     
female 2 801 -> 802         True     2      Collyer, Mrs. Harvey (Charlotte Annie Tate)   female 31        1     1     C.A. 31921      26.25         S        True     
female 2 854 -> 855         False    2      Carter, Mrs. Ernest Courtenay (Lilian Hughes) female 44        1     0     244252          26            S        True     
female 2 865 -> 866         True     2      Bystrom, Mrs. (Karolina)                      female 42        0     0     236852          13            S        True     
female 2 866 -> 867         True     2      Duran y More, Miss. Asuncion                  female 27        1     0     SC/PARIS 2149   13.8583       C        True     
female 2 874 -> 875         True     2      Abelson, Mrs. Samuel (Hannah Wizosky)         female 28        1     0     P/PP 3381       24            C        True     
female 2 880 -> 881         True     2      Shelley, Mrs. William (Imanita Parrish Hall)  female 25        0     1     230433          26            S        True     

val it: Frame<(string * int),bool> =
  
            False True 
male   3 -> 300   47   
male   1 -> 77    45   
male   2 -> 91    17   
female 3 -> 72    72   
female 1 -> 3     91   
female 2 -> 6     70
```

### Average age by class

```fsharp
titanic
|> Frame.aggregateRowsBy ["Pclass"] ["Age"] Stats.mean
```

```
val it: Frame<int,string> =
  
     Pclass Age                
0 -> 3      25.14061971830986  
1 -> 1      38.233440860215055 
2 -> 2      29.87763005780347
```

### Pivot tables

Pivot tables cross-tabulate two categorical variables with an aggregation:

```fsharp
titanic 
|> Frame.pivotTable 
    (fun k r -> r.GetAs<string>("Sex"))
    (fun k r -> r.GetAs<int>("Pclass"))
    Frame.countRows
```

```
val it: Frame<string,int> =
  
          3   1   2   
male   -> 347 122 108 
female -> 144 94  76
```

## Handling missing values

Real data has gaps. The Titanic `Age` and `Cabin` columns both contain missing
entries. Deedle makes this explicit — missing values show as `<missing>` and are
automatically skipped by statistics:

```fsharp
// How many ages are missing?
let ageCol = titanic?Age
let total = ageCol |> Series.countKeys
let present = ageCol |> Series.countValues
```

```
val ageCol: Series<int,float> =
  
0   -> 22        
1   -> 38        
2   -> 26        
3   -> 35        
4   -> 35        
5   -> <missing> 
6   -> 54        
7   -> 2         
8   -> 27        
9   -> 14        
10  -> 4         
11  -> 58        
12  -> 20        
13  -> 39        
14  -> 14        
... -> ...       
876 -> 20        
877 -> 19        
878 -> <missing> 
879 -> 56        
880 -> 25        
881 -> 33        
882 -> 22        
883 -> 28        
884 -> 25        
885 -> 39        
886 -> 27        
887 -> 19        
888 -> <missing> 
889 -> 26        
890 -> 32        

val total: int = 891
val present: int = 714
```

Fill strategies let you choose how to handle the gaps:

```fsharp
// Replace missing ages with a constant
ageCol |> Series.fillMissingWith 0.0 |> Series.take 5
```

```
val it: Series<int,float> = 
0 -> 22 
1 -> 38 
2 -> 26 
3 -> 35 
4 -> 35
```

```fsharp
// Fill forward (propagate last known value)
ageCol |> Series.fillMissing Direction.Forward |> Series.take 5
```

```
val it: Series<int,float> = 
0 -> 22 
1 -> 38 
2 -> 26 
3 -> 35 
4 -> 35
```

```fsharp
// Drop rows with missing values
ageCol |> Series.dropMissing |> Series.countKeys
```

```
val it: int = 714
```

## Creating frames and series from scratch

You can also build data frames from scratch rather than loading CSV.

```fsharp
// A simple series
let ages = series [ "Alice" => 30.0; "Bob" => 25.0; "Carol" => 35.0 ]
ages
```

```
val ages: Series<string,float> = 
Alice -> 30 
Bob   -> 25 
Carol -> 35 

val it: Series<string,float> = 
Alice -> 30 
Bob   -> 25 
Carol -> 35
```

```fsharp
// Build a frame from columns
let people = 
  Frame.ofColumns [
    "Age" => ages
    "Score" => series [ "Alice" => 90.0; "Bob" => 85.0; "Carol" => 92.0 ]
  ]
people
```

```
val people: Frame<string,string> =
  
         Age Score 
Alice -> 30  90    
Bob   -> 25  85    
Carol -> 35  92    

val it: Frame<string,string> =
  
         Age Score 
Alice -> 30  90    
Bob   -> 25  85    
Carol -> 35  92
```

```fsharp
// Build a frame from records
type Person = { Name: string; Age: int }
let records = 
  [ { Name = "Alice"; Age = 30 }
    { Name = "Bob"; Age = 25 } ]
Frame.ofRecords records
```

```
type Person =
  {
    Name: string
    Age: int
  }
val records: Person list = [{ Name = "Alice"
                              Age = 30 }; { Name = "Bob"
                                            Age = 25 }]
val it: Frame<int,string> = 
     Name  Age 
0 -> Alice 30  
1 -> Bob   25
```

## Selecting columns and rows

Pick specific columns with `Frame.sliceCols` or the indexer:

```fsharp
titanic.Columns.[ ["Name"; "Age"; "Fare"] ] |> Frame.take 3
```

```
val it: Frame<int,string> =
  
     Name                                               Age Fare    
0 -> Braund, Mr. Owen Harris                            22  7.25    
1 -> Cumings, Mrs. John Bradley (Florence Briggs Tha... 38  71.2833 
2 -> Heikkinen, Miss. Laina                             26  7.925
```

```fsharp
titanic |> Frame.sliceCols ["Pclass"; "Survived"] |> Frame.take 3
```

```
val it: Frame<int,string> =
  
     Pclass Survived 
0 -> 3      False    
1 -> 1      True     
2 -> 3      True
```

## Sorting

Sort a frame by column values:

```fsharp
titanic |> Frame.sortRowsBy "Fare" (fun (v: float) -> -v) |> Frame.take 5
```

```
val it: Frame<int,string> =
  
       PassengerId Survived Pclass Name                               Sex    Age SibSp Parch Ticket   Fare     Cabin       Embarked HasCabin 
737 -> 738         True     1      Lesurer, Mr. Gustave J             male   35  0     0     PC 17755 512.3292 B101        C        True     
258 -> 259         True     1      Ward, Miss. Anna                   female 35  0     0     PC 17755 512.3292             C        True     
679 -> 680         True     1      Cardeza, Mr. Thomas Drake Martinez male   36  0     1     PC 17755 512.3292 B51 B53 B55 C        True     
88  -> 89          True     1      Fortune, Miss. Mabel Helen         female 23  3     2     19950    263      C23 C25 C27 S        True     
341 -> 342         True     1      Fortune, Miss. Alice Elizabeth     female 24  3     2     19950    263      C23 C25 C27 S        True
```

## Further reading

This quick-start covers the most common Deedle patterns. Dive deeper with:

* [Data frame features](frame.html) — full coverage of frame construction, slicing,
grouping, aggregation with `Frame.aggregateRowsBy`, pivot tables, and more.

* [Series features](series.html) — windowing, chunking, resampling, and time-series alignment.

* [Statistics](stats.html) — moving and expanding window statistics, multi-level aggregation.

* [Handling missing values](missing.html) — sentinel types, all fill strategies, and how
missing values interact with joins.

* [Joining and merging](joining.html) — inner, outer, left, and right joins.

* [Apache Arrow / Feather](arrow.html) — zero-copy columnar I/O.

* [Deedle.MathNetNumerics](math.html) — linear algebra, correlation matrices, EWM statistics, PCA,
and linear regression via the `Deedle.MathNetNumerics` package.
