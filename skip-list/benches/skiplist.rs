#![feature(test)]

extern crate test;

use skip_list::SkipList;
use test::Bencher;
use test::black_box;

#[bench]
fn insert(b: &mut Bencher) {
    b.iter(|| {
        let mut map = SkipList::new(32);

        let mut num = 0 as u64;
        for _ in 0..1_000 {
            num = num.wrapping_mul(17).wrapping_add(255);
            map.insert(num, !num);
        }
    });
}

#[bench]
fn iter(b: &mut Bencher) {
    let mut map = SkipList::new(32);

    let mut num = 0 as u64;
    for _ in 0..1_000 {
        num = num.wrapping_mul(17).wrapping_add(255);
        map.insert(num, !num);
    }

    b.iter(|| {
        for x in map.iter() {
            black_box(x);
        }
    });
}

#[bench]
fn lookup(b: &mut Bencher) {
    let mut map = SkipList::new(32);

    let mut num = 0 as u64;
    for _ in 0..1_000 {
        num = num.wrapping_mul(17).wrapping_add(255);
        map.insert(num, !num);
    }

    b.iter(|| {
        let mut num = 0 as u64;

        for _ in 0..1_000 {
            num = num.wrapping_mul(17).wrapping_add(255);
            black_box(map.get(&num));
        }
    });
}

#[bench]
fn get_updates(b: &mut Bencher) {
    let mut map = SkipList::new(32);

    let mut num = 0 as u64;
    for _ in 0..1_000 {
        num = num.wrapping_mul(17).wrapping_add(255);
        map.insert(num, !num);
    }

    b.iter(|| {
        let mut num = 0 as u64;

        for _ in 0..1_000 {
            num = num.wrapping_mul(17).wrapping_add(255);
            black_box(map.get_updates_for_bench(&num));
        }
    });
}

#[bench]
fn insert_remove(b: &mut Bencher) {
    b.iter(|| {
        let mut map = SkipList::new(32);

        let mut num = 0 as u64;
        for _ in 0..1_000 {
            num = num.wrapping_mul(17).wrapping_add(255);
            map.insert(num, !num);
        }

        let mut num = 0 as u64;
        for _ in 0..1_000 {
            num = num.wrapping_mul(17).wrapping_add(255);
            black_box(map.remove(&num).unwrap());
        }
    });
}
