size(16cm);

real u = 2;
real w = 20; 
real h = 2;
real b = 1.5;

draw((0,0)--((w - 1)*u,0)--((w - 1)*u,h)--(0,h)--cycle);

for (int j = 0; j < w - 1; ++j) {
    draw((j*u,0)--(j*u,h));
    label(format("%d", j), (j * u + u / 2, 0), S);
}
label("$\dots$",((w - 0.5) * u, h / 2));

draw((0, -b)--(5*u, -b)--(5*u, h+b)--(0, h+b)--cycle, dashed);
label("row\_id", (2.5*u, h+b), N);
draw((5*u, -b)--(8*u, -b)--(8*u, h+b)--(5*u, h+b), dashed);
label("CHROM", (6.5*u, h+b), N);
draw((8*u, -b)--(13*u,-b)--(13*u, h+b)--(8*u, h+b), dashed);
label("POS", (10.5*u, h+b), N);

draw((5*u, 0)--(7*u, 0)--(7*u, h)--(5*u, h)--cycle, red);
draw((6u, h/2)..(9u, h+b/2)..(13.5u, h/2), EndArrow);
dot((6u, h/2));

draw("Fixed region", (0, -2b)--(13u, -2b), Arrows);
draw("Variable region", (13u, -2b)--((w - 1) * u, -2b), BeginArrow);
label("$\dots$",((w - 0.5) * u, -2b));

