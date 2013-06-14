
real w = 80mm; 
pair[] n;


n[1] = (0,0);
n[2] = (w / 2,0);
n[3] = (w,0);
n[4] = (w/3,w/3);
n[5] = (2w/3,2w/3);

int i;
for (i = 1; i < 6; ++i) {
    dot(format("%d", i), n[i]);
}

draw(n[1]--n[4]);
draw(n[2]--n[4]);
draw(n[3]--n[5]);
draw(n[4]--n[5]);

draw("30441.57", (-w/20, 0) -- (-w/20, w/3), W, Arrows);
draw("46750.11", (w + w/20, 0) -- (w + w/20, 2w/3), Arrows);

