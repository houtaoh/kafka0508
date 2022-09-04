package com.atguigu;

/**
 * Created by 94478 on 2022/8/29.
 */
public class Main {

    public static void main(String[] args) {
        TreeNode node1 = new TreeNode();
        TreeNode node2 = new TreeNode();
        System.out.println(dfs(node1,node2));
        // 11
    }
    public static boolean dfs(TreeNode node1,TreeNode node2){
        if(node1.val == node2.val && (node1.left == null && node1.right == null
                && node2.left == null && node2.right == null)){
            return true;
        }
        TreeNode node1_left = node1.left;
        TreeNode node2_left = node2.left;
        TreeNode node1_right = node1.right;
        TreeNode node2_right = node2.right;
        if (node1_left.val == node2_left.val && node1_right.val == node2_right.val){
            return dfs(node1_left,node2_left) && dfs(node1_right,node2_right);
        }else{
            return false;
        }
    }
}
