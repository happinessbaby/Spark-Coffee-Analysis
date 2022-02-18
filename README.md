BEVERAGES CONSUMER ANALYSIS


For this project, I first used python to find patterns in the data. With my python codes, I found that there are patterns existing in not only the Branches data but also in the Consumer Counts data.
More specifically, the consumer counts data are in patterns of XYXY, where X being a block of one "menu" of beverages while Y being another "menu". Although each X block across A, B, C are not comprised of the same beverages, within A, for example, the X block beverages repeat for 16 times, and the same goes for Y. One interpretation could be that each block represents one day of sale, and beverages on that block are sold on the menu of the branches.

Another interesting find is that certain beverages appear twice as many times as other beverages. One explanation could be that beverages including SMALL_cappuccino, MED_cappuccino, LARGE_cappuccino, COLD_cappuccino, ICY_cappuccino, Triple_cappuccino, Mild_cappuccino are sold daily while other beverages that do not appear on every block are sold every other day. I delved into this pattern in my future query, along with a primitive attempt at doing a correlation analysis with Spark MLlib.

For the core of the project, I give the client a choice in viewing which branches they want to query into, instead of hard-coding the solutions.


