
/***************************************************************************************
 *
 * Copyright (c) 2018 Lejent, Inc. All Rights Reserved
 *
 **************************************************************************************/
/**
 *    @file: bind.cpp
 *  @author: zhangxingbiao(xingbiao.zhang@lejent.com)
 *    @date: 04/10/2018 11:39:26 AM
 * @version: 1.0 
 *   @brief: 
 *  
 **/
#include <stdio.h>
#include <vector>
using namespace std;

template<class T> class type {};

template< int I > struct arg{}; 

template<class T> class value {
public:                                                                                                              
    value(T const & t): t_(t) {}                                                                                     
    T & get() { return t_; }
    T const & get() const { return t_; }                                                                             
private:                                                                                                             
    T t_;                                                                                                            
};

template<class A1> struct storage1 {
    explicit storage1( A1 a1 ): a1_( a1 ) {}
    A1 a1_;
}; 

template<class A1, class A2> struct storage2 : public storage1<A1> { 
    explicit storage2( A1 a1, A2 a2 ): storage1<A1>( a1 ), a2_(a2) {}
    A2 a2_;
}; 

class list0 {
public:
    list0() {}

    template<class T> T & operator[] (value<T> & v) const { return v.get(); }
    
    template<class R, class F, class A> R operator()(type<R>, F & f, A &){
        return f();
    }
};

template< class A1 > class list1: private storage1< A1 >{
private: 
    typedef storage1< A1 > base_type;                                                                                
public:                                                                                                              
    explicit list1( A1 a1 ): base_type( a1 ) { }                                                                                                                                    
    A1 operator[] (arg<1>) const { 
      return base_type::a1_; 
    }                                                                                                                     
    //传的值调用这个
    template<class T> T & operator[] ( value<T> & v ) const { 
      return v.get(); 
    }
    
    template<class R, class F, class A> R operator()(type<R>, F & f, A & a){   
        return f(a[base_type::a1_]);
    }
};

template< class A1, class A2 > class list2: private storage2< A1, A2 > {
private: 
    typedef storage2< A1, A2 > base_type;

public:                                                                                                              
    explicit list2( A1 a1, A2 a2 ): base_type( a1, a2 ) { }                                                                                                                                    
    //传的占位符调用这个
    A1 operator[] (arg<1>) const { 
      return base_type::a1_; 
    }                                                                                                                 
    A2 operator[] (arg<2>) const { 
      return base_type::a2_; 
    }  
    //传的值调用这个
    template<class T> T & operator[] ( value<T> & v ) const { 
      return v.get(); 
    }
    
    template<class R, class F, class A> R operator()(type<R>, F & f, A & a) {   
        return f(a[base_type::a1_], a[base_type::a2_]);
    }
};

template<class R, class F, class L> class bind_t {
public:

    bind_t(F f, L const & l): f_(f), l_(l) {}

    typedef R result_type;

    result_type operator()() { 
        list0 a; 
        return l_(type<R>(), f_, a); 
    }
        
    template<class A1> result_type operator()(A1 & a1) {
        list1<A1 &> a(a1);
        return l_(type<R>(), f_, a);
    }
    
    template<class A1> result_type operator()(A1 const & a1) {
        list1<A1 const &> a(a1);
        return l_(type<R>(), f_, a);
    }
    
    template<class A1, class A2> result_type operator()(A1 & a1, A2 &a2) {
        list2<A1 &, A2&> a(a1, a2);
        return l_(type<R>(), f_, a);
    }

    template<class A1, class A2> result_type operator()(A1 const & a1, A2 const &a2) {
        list2<A1 const &, A2 const &> a(a1, a2);
        return l_(type<R>(), f_, a);
    }

  private:

    F f_;
    L l_;
};


template<class R, class T> class mf0 {
public:
    typedef R result_type;
    typedef T * argument_type;
private:
    
    typedef R ( T::*F) ();
    F f_;

    template<class U> R call(U & u, T const *) const {
        return (u.*f_)();
    }

public:
    
    explicit mf0(F f): f_(f) {}

    R operator()(T * p) const {
        return (p->*f_)();
    }

    template<class U> R operator()(U & u) const {
        U const * p = 0;
        return call(u, p);
    }

    R operator()(T & t) const {
        return (t.*f_)();
    }
};

template<class R, class T, class A1> class mf1 {
public:

    typedef R result_type;
    typedef T * first_argument_type;
    typedef A1 second_argument_type;

private:
    
    typedef R ( T::*F) (A1);
    F f_;

    template<class U, class B1> R call(U & u, T const *, B1 & b1) const {
        return (u.*f_)(b1);
    }

public:
    
    explicit mf1(F f): f_(f) {}

    R operator()(T * p, A1 a1) const {
        return (p->*f_)(a1);
    }

    template<class U> R operator()(U & u, A1 a1) const {
        U const * p = 0;
        return call(u, p, a1);
    }

    R operator()(T & t, A1 a1) const {
        return (t.*f_)(a1);
    }
};

template<class T> struct add_value {   
    typedef value<T> type;
};

template<int I> struct add_value< arg<I> > {   
    typedef arg<I> type;
};

template<class A1> struct list_av_1 {
    typedef typename add_value<A1>::type B1;
    typedef list1<B1> type;
};

template<class A1, class A2> struct list_av_2 {
    typedef typename add_value<A1>::type B1;
    typedef typename add_value<A2>::type B2;
    typedef list2<B1, B2> type;
};

//仿函数 
template<class R, class F> bind_t<R, F, list0> bind(F f) {
  typedef list0 list_type;
  return bind_t<R, F, list_type>(f, list_type());
}

template<class R, class F, class A1>
    bind_t<R, F, typename list_av_1<A1>::type >
    bind(F f, A1 a1) {
    typedef typename list_av_1<A1>::type list_type;
    return bind_t<R, F, list_type> (f, list_type(a1));
}

template<class R, class F, class A1, class A2>
    bind_t<R, F, typename list_av_2<A1, A2>::type >
    bind(F f, A1 a1, A2 a2) {
    typedef typename list_av_2<A1, A2>::type list_type;
    return bind_t<R, F, list_type>(f, list_type(a1, a2));
}

//函数指针
template<class R>
    bind_t<R, R (*) (), list0>
    bind(R (*f) ()) {
    typedef R (*F) ();
    typedef list0 list_type;
    return bind_t<R, F, list_type> (f, list_type());
}

template<class R, class B1, class A1>
    bind_t<R, R (*) (B1), typename list_av_1<A1>::type >
    bind(R (*f) (B1), A1 a1) {
    typedef R (*F) (B1);
    typedef typename list_av_1<A1>::type list_type;
    return bind_t<R, F, list_type> (f, list_type(a1));
}

template<class R, class B1, class B2, class A1, class A2>
    bind_t<R, R (*) (B1, B2), typename list_av_2<A1, A2>::type >
    bind(R (*f) (B1, B2), A1 a1, A2 a2) {
    typedef R (*F) (B1, B2);
    typedef typename list_av_2<A1, A2>::type list_type;
    return bind_t<R, F, list_type> (f, list_type(a1, a2));
}

//类成员函数
template<class R, class T, class A1>
    bind_t<R, mf0<R, T>, typename list_av_1<A1>::type >
    bind(R (T::*f) (), A1 a1) {
    typedef mf0<R, T> F;
    typedef typename list_av_1<A1>::type list_type;
    return bind_t<R, F, list_type>(F(f), list_type(a1));
}

template<class R, class T,
    class B1,
    class A1, class A2>
    bind_t<R, mf1<R, T, B1>, typename list_av_2<A1, A2>::type >
    bind(R (T::*f) (B1), A1 a1, A2 a2) {
    typedef mf1<R, T, B1> F;
    typedef typename list_av_2<A1, A2>::type list_type;
    return bind_t<R, F, list_type>(F(f), list_type(a1, a2));
}

struct AddOp{
  AddOp() {}
  int operator()(int a, int b) {
    return a + b;
  }
};

int add10(int a) {
  return a + 10;
}

int add(int a, int b) {
  return a + b;
}

class Calculator{
  public:
    Calculator() {}

    int add10(int a) {
      return a + 10;
    }
};


int main() {
  arg<1> _1;
  arg<2> _2;
  
  //仿函数比较特殊，必须手动指定返回值类型,本代码对仿函数处理不正确
  AddOp addop;
  printf("%d\n", bind<int, AddOp, arg<1>, arg<2> >(addop, _1, _2)(1, 2));
  printf("%d\n", bind<int>(addop, _1, _2)(1, 2));
  //普通函数 
  printf("%d\n", bind(add, 2, 3)());
  printf("%d\n", bind(add, _1, _2)(2, 3));
  //类成员函数
  Calculator caltor;
  printf("%d\n", bind(&Calculator::add10, &caltor, _1)(1));
  printf("%d\n", bind(&Calculator::add10, _1, _2)(&caltor, 1));
  return 0;
}
