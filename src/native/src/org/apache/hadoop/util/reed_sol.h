/* reed_sol.h
 * James S. Plank

Jerasure - A C/C++ Library for a Variety of Reed-Solomon and RAID-6 Erasure Coding Techniques
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
Department of Electrical Engineering and Computer Science
University of Tennessee
Knoxville, TN 37996
plank@cs.utk.edu
*/

/*
 * $Revision: 1.2 $
 * $Date: 2008/08/19 17:40:58 $
 */

extern int *reed_sol_vandermonde_coding_matrix(int k, int m, int w);
extern int *reed_sol_extended_vandermonde_matrix(int rows, int cols, int w);
extern int *reed_sol_big_vandermonde_distribution_matrix(int rows, int cols, int w);

extern int reed_sol_r6_encode(int k, int w, char **data_ptrs, char **coding_ptrs, int size);
extern int *reed_sol_r6_coding_matrix(int k, int w);

extern void reed_sol_galois_w08_region_multby_2(char *region, int nbytes);
extern void reed_sol_galois_w16_region_multby_2(char *region, int nbytes);
extern void reed_sol_galois_w32_region_multby_2(char *region, int nbytes);
