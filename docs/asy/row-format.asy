size(16cm);

real u = 2;
real w = 20; 
real h = 2;
real b = 1;

draw((0,0)--(w*u,0)--(w*u,h)--(0,h)--cycle);

for (int j = 0; j < w; ++j) {
    draw((j*u,0)--(j*u,h));
    label(format("%d", j), (j * u + u / 2, 0), S);
}

draw((0, -b)--(2*u, -b)--(2*u, h+b)--(0, h+b)--cycle, dashed);
label("row\_id", (u, h+b), N);

