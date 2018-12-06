pub fn divide(n: usize, m: usize) -> Vec<usize> {
    let avg = n / m;
    let remain = n % m;
    let mut c = Vec::new();
    let mut i = 0;
    while i < m {
        if i < remain {
            c.push(avg + 1);
        } else {
            c.push(avg);
        }
        i = i + 1;
    }
    c
}
