/* Galois.h
 * James S. Plank

Galois.tar - Fast Galois Field Arithmetic Library in C/C++
Copright (C) 2007 James S. Plank

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

James S. Plank
Department of Computer Science
University of Tennessee
Knoxville, TN 37996
plank@cs.utk.edu

 */

/*
 * Part of jerasure.h: 
 * $Revision: 1.2 $
 * $Date: 2008/08/19 17:40:58 $
 */

#ifndef _GALOIS_H
#define _GALOIS_H

#include <stdio.h>
#include <stdlib.h>

extern int galois_single_multiply(int a, int b, int w);
extern int galois_single_divide(int a, int b, int w);
extern int galois_log(int value, int w);
extern int galois_ilog(int value, int w);

extern int galois_create_log_tables(int w);   /* Returns 0 on success, -1 on failure */
extern int galois_logtable_multiply(int x, int y, int w);
extern int galois_logtable_divide(int x, int y, int w);

extern int galois_create_mult_tables(int w);   /* Returns 0 on success, -1 on failure */
extern int galois_multtable_multiply(int x, int y, int w);
extern int galois_multtable_divide(int x, int y, int w);

extern int galois_shift_multiply(int x, int y, int w);
extern int galois_shift_divide(int x, int y, int w);

extern int galois_create_split_w8_tables();
extern int galois_split_w8_multiply(int x, int y);

extern int galois_inverse(int x, int w);
extern int galois_shift_inverse(int y, int w);

extern int *galois_get_mult_table(int w);
extern int *galois_get_div_table(int w);
extern int *galois_get_log_table(int w);
extern int *galois_get_ilog_table(int w);

void galois_region_xor(           char *r1,         /* Region 1 */
                                  char *r2,         /* Region 2 */
                                  char *r3,         /* Sum region (r3 = r1 ^ r2) -- can be r1 or r2 */
                                  int nbytes);      /* Number of bytes in region */

/* These multiply regions in w=8, w=16 and w=32.  They are much faster
   than calling galois_single_multiply.  The regions must be long word aligned. */

void galois_w08_region_multiply(char *region,       /* Region to multiply */
                                  int multby,       /* Number to multiply by */
                                  int nbytes,       /* Number of bytes in region */
                                  char *r2,         /* If r2 != NULL, products go here.  
                                                       Otherwise region is overwritten */
                                  int add);         /* If (r2 != NULL && add) the produce is XOR'd with r2 */

void galois_w16_region_multiply(char *region,       /* Region to multiply */
                                  int multby,       /* Number to multiply by */
                                  int nbytes,       /* Number of bytes in region */
                                  char *r2,         /* If r2 != NULL, products go here.  
                                                       Otherwise region is overwritten */
                                  int add);         /* If (r2 != NULL && add) the produce is XOR'd with r2 */

void galois_w32_region_multiply(char *region,       /* Region to multiply */
                                  int multby,       /* Number to multiply by */
                                  int nbytes,       /* Number of bytes in region */
                                  char *r2,         /* If r2 != NULL, products go here.  
                                                       Otherwise region is overwritten */
                                  int add);         /* If (r2 != NULL && add) the produce is XOR'd with r2 */

#endif
